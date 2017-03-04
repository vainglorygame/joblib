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

    async def _execute_job(self, jobid, payload, priority):
        # override
        pass

    async def _work(self):
        """Fetch a job and run it."""
        jobid, payload, priority = await self._queue.acquire(
            jobtype=self._jobtype)
        if jobid is None:
            raise LookupError("no jobs available")
        try:
            await self._execute_job(jobid, payload, priority)
            await self._queue.finish(jobid)
        except JobFailed as error:
            logging.warning("%s: failed with %s", jobid,
                            error.args[0])
            await self._queue.fail(jobid, error.args[0])

    async def run(self):
        """Start jobs forever."""
        while True:
            try:
                await self._work()
            except LookupError:
                await asyncio.sleep(1)

    async def start(self, number=1):
        """Start jobs in background."""
        for _ in range(number):
            asyncio.ensure_future(self.run())
