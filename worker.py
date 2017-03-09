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
            jobs = await self._queue.acquire(jobtype=self._jobtype,
                                       length=batchlimit)

            if len(jobs) == 0:
                await asyncio.sleep(0.1)
                # nothing to do
                continue

            await self._windup()
            critical_error = True
            failed = []
            for jobid, payload, priority in jobs:
                try:
                    await self._execute_job(jobid, payload, priority)
                except JobFailed as err:
                    failed.append((jobid, err.args[0]))
                    if len(err.args) > 1:
                        # rollback
                        critical_error = err.args[1]
                        if critical_error:
                            break
            if critical_error:
                await self._queue.reset([j[0] for j in jobs])
                logging.warning("batch failed, reset")
            else:
                await self._queue.finish([j[0] for j in jobs])

            for jobid, err in failed:
                await self._queue.fail(jobid, err)

            await self._teardown(failed=critical_error)

    async def start(self, batchlimit=1):
        """Start in background."""
        asyncio.ensure_future(self.run(batchlimit))
