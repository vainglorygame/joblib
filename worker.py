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
        self._need_update = True
        self._children = []

    async def connect(self, **queuedb):
        self._queue = joblib.joblib.JobQueue()
        # register event handler on new open job
        await self._queue.listen(
            self._jobtype + "_open",
            self._pushed_new)
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

    async def request(self, jobtype, payload, priority=None):
        """Queue child jobs to be executed after teardown."""
        self._children.append((jobtype, payload, priority))

    async def poll(self, batchlimit=1):
        """Start a batch of jobs."""
        jobs = await self._queue.acquire(jobtype=self._jobtype,
                                         length=batchlimit)

        if len(jobs) == 0:
            logging.debug("no jobs")
            self._need_update = False
            return

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
            await self._queue.reset(succeeded,
                                    self._jobtype)
            logging.warning("batch failed, reset")
        else:
            await self._queue.finish(succeeded,
                                     self._jobtype)

        for jobid, err in failed:
            await self._queue.fail(jobid,
                                   self._jobtype,
                                   err)

        await self._teardown(failed=critical_error)

        # spawn child jobs
        for jobt, payloads, priorities in self._children:
            await self._queue.request(
                jobt, payloads, priorities)
        self._children = []

    def _pushed_new(self, _):
        """New job event handler."""
        logging.debug("received a notification")
        self._need_update = True

    async def run(self, batchlimit):
        """Request a poll after a push or a timer."""
        while True:
            while not self._need_update:
                await asyncio.sleep(0.01)
            logging.debug("polling")
            await self.poll(batchlimit)

    async def start(self, batchlimit=1):
        """Start in background."""
        asyncio.ensure_future(self.run(batchlimit))
