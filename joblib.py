#!/usr/bin/python3

import asyncio
import asyncpg
import json
import logging


class JobQueue(object):
    def __init__(self):
        self._pool = None

    async def connect(self, **args):
        """Connect the database."""
        logging.info("connecting to queue database")
        while True:
            try:
                self._pool = await asyncpg.create_pool(
                    min_size=1, **args)
                break
            except asyncpg.exceptions.CannotConnectNowError:
                logging.warning(
                    "queue database is not ready yet, retrying")
                await asyncio.sleep(1)
            except asyncpg.exceptions.TooManyConnectionsError:
                logging.warning(
                    "queue database has too many clients, retrying")
                await asyncio.sleep(1)

    async def setup(self):
        """Initialize the database."""
        async with self._pool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS
                jobs (
                    id SERIAL PRIMARY KEY,
                    priority INT DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    type TEXT,
                    payload JSONB
                )
                """)
            await con.execute("""
                CREATE UNIQUE INDEX ON jobs(priority, id)
            """)

    async def request(self, jobtype, payload, priority=0):
        """Create a new job and return its id."""
        async with self._pool.acquire() as con:
            insert = await con.prepare("""
                    INSERT INTO jobs(type, payload, priority)
                    VALUES($1, $2, $3)
                    RETURNING id
            """)
            if isinstance(payload, list):
                payloads = payload
            else:
                payloads = [payload]
            if isinstance(priority, list):
                priorities = priority
            else:
                priorities = [priority] * len(payloads)
            ids = []
            async with con.transaction():
                for pl, pr in zip(payloads, priorities):
                    ids.append(await insert.fetchval(jobtype,
                                                     json.dumps(pl),
                                                     pr))
                await con.execute("SELECT pg_notify($1 || '_open', '')",
                                  jobtype)
            if isinstance(payload, list):
                return ids
            else:
                return ids[0]

    async def acquire(self, jobtype, length=None):
        """Mark a job as running, return id, payload and priority.
        Return (None, None, None) if no job is available."""
        async with self._pool.acquire() as con:
            update = await con.prepare("""
                UPDATE jobs SET STATUS='running'
                FROM (
                    SELECT id FROM jobs
                    WHERE status='open' AND type=$1
                    ORDER BY priority, id
                    LIMIT $2
                ) AS open_jobs
                WHERE jobs.id=open_jobs.id
                RETURNING jobs.id, jobs.payload, jobs.priority
            """)
            if length is None:
                limit = 1
            else:
                limit = length
            while True:
                try:
                    # do not allow async access
                    async with con.transaction(isolation="serializable"):
                        result = await update.fetch(jobtype, limit)
                        if len(result) == 0 and length is None:
                            # no jobs available
                            # backwards compatibility
                            return None, None, None

                        jobs = []
                        for record in result:
                            jobs.append((record[0],
                                         json.loads(record[1]),
                                         record[2]))

                        if length is None:
                            return jobs[0]
                        else:
                            return jobs
                except asyncpg.exceptions.SerializationError:
                    # job is being picked up by another worker, try again
                    logging.debug("serialization error, retrying")
                    pass

    async def status(self, jobid):
        """Return the status of a job."""
        async with self._pool.acquire() as con:
            return await con.fetchval("""
                SELECT status
                FROM jobs WHERE
                id=$1
            """, jobid)

    async def finish(self, jobid):
        """Mark jobs as completed."""
        async with self._pool.acquire() as con:
            update = await con.prepare("""
                UPDATE jobs
                SET status='finished'
                WHERE id=$1
            """)
            if not isinstance(jobid, list):
                jobids = [jobid]
            else:
                jobids = jobid
            async with con.transaction():
                for jid in jobids:
                    await update.fetch(jid)

    async def fail(self, jobid, reason):
        """Mark a job as failed."""
        async with self._pool.acquire() as con:
            update = await con.prepare("""
                UPDATE jobs
                SET status='failed', payload=payload||$2::jsonb
                WHERE id=$1
            """)
            if not isinstance(jobid, list):
                jobids = [jobid]
            else:
                jobids = jobid
            if not isinstance(reason, list):
                reasons = [reason]
            else:
                reasons = reason
            assert len(jobids) == len(reasons)
            async with con.transaction():
                for jid, rsn in zip(jobids, reasons):
                    await update.fetch(jid,
                                       json.dumps({"error": rsn}))

    async def reset(self, jobid):
        """Mark a job as open."""
        async with self._pool.acquire() as con:
            update = await con.prepare("""
                UPDATE jobs
                SET status='open'
                WHERE id=$1
            """)
            if not isinstance(jobid, list):
                jobids = [jobid]
            else:
                jobids = jobid
            async with con.transaction():
                for jid in jobids:
                    await update.fetch(jid)

    async def cleanup(self):
        """Reopen all unfinished jobs."""
        async with self._pool.acquire() as con:
            while True:
                try:
                    async with con.transaction(isolation="serializable"):
                        await con.execute("""
                            UPDATE jobs
                            SET status='open'
                            WHERE status='running'
                        """)
                        return
                except asyncpg.exceptions.SerializationError:
                    pass
