#!/usr/bin/python3

import asyncio
import asyncpg
import json


class JobQueue(object):
    def __init__(self):
        self._pool = None

    async def connect(self, **args):
        """Connect the database."""
        self._pool = await asyncpg.create_pool(**args)

    async def setup(self):
        """Initialize the database."""
        async with self._pool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS
                jobs (
                    id SERIAL,
                    priority INT DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    type TEXT,
                    payload JSONB
                )
                """)

    async def request(self, jobtype, payload, priority=0):
        """Create a new job."""
        async with self._pool.acquire() as con:
            await con.execute("""
                INSERT INTO jobs(type, payload, priority)
                VALUES($1, $2, $3)
            """, jobtype, json.dumps(payload), priority)

    async def acquire(self, jobtype):
        """Mark a job as running, return id, payload and priority.
        Return (None, None, None) if no job is available."""
        async with self._pool.acquire() as con:
            while True:
                try:
                    # do not allow async access
                    async with con.transaction(isolation="serializable"):
                        result = await con.fetchrow("""
                            SELECT id, payload, priority
                            FROM jobs WHERE
                            type=$1 AND status='open'
                            ORDER BY priority DESC
                        """, jobtype)
                        if result is None:
                            # no jobs available
                            return None, None, None
                        jobid, payload, priority = result
                        await con.execute("""
                            UPDATE jobs
                            SET status='running'
                            WHERE id=$1
                        """, jobid)
                        return jobid, json.loads(payload), priority
                except asyncpg.exceptions.SerializationError:
                    # job is being picked up by another worker, try again
                    pass

    async def finish(self, jobid):
        """Mark a job as completed."""
        async with self._pool.acquire() as con:
            while True:
                try:
                    async with con.transaction(isolation="serializable"):
                        await con.execute("""
                            UPDATE jobs
                            SET status='finished'
                            WHERE id=$1
                        """, jobid)
                        return
                except asyncpg.exceptions.SerializationError:
                    pass

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
