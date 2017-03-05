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

    async def _work(self):
        """Fetch a job and run it.
        Return id."""
        jobid, payload, priority = await self._queue.acquire(
            jobtype=self._jobtype)
        if jobid is None:
            raise LookupError
        try:
            await self._execute_job(jobid, payload, priority)
        except JobFailed as err:
            raise JobFailed(err.args[0], jobid)
        return jobid

    async def run(self, batchlimit=1):
        """Start jobs forever."""
        while True:
            await self._windup()
            jobids = []
            error = None
            low_load = False
            try:
                for _ in range(batchlimit):
                    try:
                        jobids.append(await self._work())
                    except LookupError:
                        low_load = True
                        break
                    except JobFailed as err:
                        error = err.args[0]
                        jobids.append(err.args[1])
                        break
            finally:
                if error is not None:
                    await self._queue.reset(jobids[:-1])
                    logging.debug(jobids)
                    await self._queue.fail(jobids[-1], error)
                    logging.warning("batch failed, reset")
                else:
                    await self._queue.finish(jobids)
                await self._teardown(failed=error is not None)
            if low_load:
                await asyncio.sleep(0.1)

    async def start(self, batchlimit=1):
        """Start in background."""
        asyncio.ensure_future(self.run(batchlimit))
