#!/usr/bin/python3

import asyncio
import asyncpg
import json
import logging


class JobQueue(object):
    def __init__(self):
        self._con = None
        self._listens = {}

    def _listener(self, con, pid,
                        channel, payload):
        """Fire the registered callback. Must not be async."""
        self._listens[channel](payload)

    async def _hook_listener(self):
        """Register callbacks."""
        if len(self._listens.keys()) > 0:
            for channel in self._listens.keys():
                await self._con.add_listener(channel,
                                             self._listener)

    async def connect(self, **args):
        """Connect the database."""
        logging.info("connecting to queue database")
        while True:
            try:
                self._con = await asyncpg.connect(**args)
                await self._hook_listener()
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
        await self._con.execute("""
            CREATE TABLE IF NOT EXISTS
            jobs (
                id SERIAL PRIMARY KEY,
                priority INT DEFAULT 0,
                status TEXT DEFAULT 'open',
                type TEXT,
                payload JSONB
            )
        """)
        await self._con.execute("""
            CREATE UNIQUE INDEX ON jobs(priority, id)
        """)

    async def listen(self, channel, callback):
        """Hook a notification listener."""
        self._listens[channel] = callback

    async def request(self, jobtype, payload, priority=0):
        """Create a new job and return its id."""
        if isinstance(payload, list):
            payloads = [json.dumps(p)
                        for p in payload]
        else:
            payloads = [json.dumps(payload)]
        if isinstance(priority, list):
            priorities = priority
        else:
            priorities = [priority] * len(payloads)
        insert = await self._con.prepare("""
            INSERT INTO jobs(type, payload, priority)
            VALUES($1, $2, $3)
            RETURNING id
        """)
        ids = []
        async with self._con.transaction():
            for pl, pr in zip(payloads, priorities):
                ids.append(await insert.fetch(jobtype, pl, pr))
            await self._con.execute("SELECT pg_notify($1 || '_open', '')",
                                    jobtype)

        if isinstance(payload, list):
            return ids
        else:
            return ids[0]

    async def acquire(self, jobtype, length=None):
        """Mark a job as running, return id, payload and priority.
        Return (None, None, None) if no job is available."""
        if length is None:
            limit = 1
        else:
            limit = length
        while True:
            try:
                # do not allow async access
                async with self._con.transaction(isolation="serializable"):
                    result = await self._con.fetch("""
                        UPDATE jobs SET STATUS='running'
                        FROM (
                            SELECT id FROM jobs
                            WHERE status='open' AND type=$1
                            ORDER BY priority, id
                            LIMIT $2
                        ) AS open_jobs
                        WHERE jobs.id=open_jobs.id
                        RETURNING jobs.id, jobs.payload, jobs.priority
                    """, jobtype, limit)
                    if len(result) == 0 and length is None:
                        # no jobs available
                        # backwards compatibility
                        return None, None, None

                    jobs = [(r[0], json.loads(r[1]), r[2]) for r in result]

                    await self._con.execute("SELECT pg_notify($1 || '_running', '')",
                                            jobtype)

                    if length is None:
                        return jobs[0]
                    else:
                        return jobs
            except asyncpg.exceptions.SerializationError:
                # job is being picked up by another worker, try again
                pass

    async def status(self, jobid):
        """Return the status of a job."""
        return await self._con.fetchval("""
            SELECT status
            FROM jobs WHERE
            id=$1
        """, jobid)

    async def finish(self, jobid, jobtype):
        """Mark jobs as completed."""
        if not isinstance(jobid, list):
            jobids = [(jobid,)]
        else:
            jobids = [(jid,) for jid in jobid]
        async with self._con.transaction():
            await self._con.executemany("""
                UPDATE jobs SET status='finished'
                WHERE id=$1
            """, jobids)
            await self._con.execute("""
                SELECT pg_notify($1 || '_finished', '')
            """, jobtype)

    async def fail(self, jobid, jobtype, reason):
        """Mark a job as failed."""
        if not isinstance(jobid, list):
            jobids = [jobid]
        else:
            jobids = jobid
        if not isinstance(reason, list):
            reasons = [json.dumps({"error": reason})]
        else:
            reasons = [json.dumps({"error": r})
                       for r in reason]
        assert len(jobids) == len(reasons)
        async with self._con.transaction():
            await self._con.executemany("""
                UPDATE jobs SET status='failed',
                    payload=payload||$2::jsonb
                WHERE id=$1
            """, zip(jobids, reasons))
            await self._con.execute("""
                SELECT pg_notify($1 || '_failed', '')
            """, jobtype)

    async def reset(self, jobid, jobtype):
        """Mark a job as open."""
        if not isinstance(jobid, list):
            jobids = [(jobid,)]
        else:
            jobids = [(jid,) for jid in jobid]
        async with self._con.transaction():
            await self._con.executemany("""
                UPDATE jobs SET status='open'
                WHERE id=$1
            """, jobids)
            await self._con.execute("""
                SELECT pg_notify($1 || '_open', '')
            """, jobtype)

    async def cleanup(self):
        """Reopen all unfinished jobs."""
        while True:
            try:
                async with self._con.transaction(isolation="serializable"):
                    await self._con.execute("""
                        UPDATE jobs
                        SET status='open'
                        WHERE status='running'
                    """)
                    return
            except asyncpg.exceptions.SerializationError:
                pass
