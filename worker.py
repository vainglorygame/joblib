#!/usr/bin/python

import asyncio
import logging
import joblib.joblib


class JobFailed(Exception):
    pass


class Worker(object):
    """Abstract service worker class."""
    def __init__(self, jobtype):
        self._queue = None
        self._jobtype = jobtype

    async def connect(self, **queuedb):
        self._queue = joblib.joblib.JobQueue()
        await self._queue.connect(**queuedb)
        await self._queue.setup()

    async def setup(self):
        # override
        pass

    async def _windup(self):
        # override
        pass

    async def _execute_job(self, jobid, payload, priority):
        # override
        pass

    async def _teardown(self, failed):
        # override
        pass

    async def run(self, batchlimit=1):
        """Start jobs forever."""
        while True:
            await self._windup()
            jobs = await self._queue.acquire(jobtype=self._jobtype,
                                       length=batchlimit)

            if len(jobs) == 0:
                await asyncio.sleep(0.1)
                # nothing to do
                continue

            error = None
            try:
                for jobid, payload, priority in jobs:
                    await self._execute_job(jobid, payload, priority)
            except JobFailed as err:
                error = err.args[0]
            finally:
                if error is not None:
                    await self._queue.reset([j[0] for j in jobs])
                    await self._queue.fail(jobid, error)
                    logging.warning("batch failed, reset")
                    await self._teardown(failed=True)
                else:
                    await self._queue.finish([j[0] for j in jobs])
                    await self._teardown(failed=False)

    async def start(self, batchlimit=1):
        """Start in background."""
        asyncio.ensure_future(self.run(batchlimit))
