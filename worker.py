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
                await asyncio.sleep(1)
                # nothing to do
                continue

            await self._windup()
            critical_error = False
            failed = []
            succeeded = []
            for jobid, payload, priority in jobs:
                try:
                    await self._execute_job(jobid, payload, priority)
                    succeeded.append(jobid)
                except JobFailed as err:
                    failed.append((jobid, err.args[0]))
                    if len(err.args) > 1:
                        # rollback
                        critical_error = err.args[1]
                        if critical_error:
                            break
                    else:
                        critical_error = True  # default
            if critical_error:
                await self._queue.reset(succeeded)
                logging.warning("batch failed, reset")
            else:
                await self._queue.finish(succeeded)

            for jobid, err in failed:
                await self._queue.fail(jobid, err)

            await self._teardown(failed=critical_error)

    async def start(self, batchlimit=1):
        """Start in background."""
        asyncio.ensure_future(self.run(batchlimit))
