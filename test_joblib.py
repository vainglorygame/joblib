#!/usr/bin/python3

import os
import asyncio
import json
import asyncpg
import pytest
import joblib

class TestJoblib:
    # async fixtures are not available yet, so we use a workaround
    async def queue_helper(self, q):
        await q.connect(
            host=os.environ["POSTGRESQL_HOST"],
            port=os.environ["POSTGRESQL_PORT"],
            user=os.environ["POSTGRESQL_USER"],
            password=os.environ["POSTGRESQL_PASSWORD"],
            database=os.environ["POSTGRESQL_DB"]
        )
        async with q._pool.acquire() as con:
            await con.execute("DROP TABLE IF EXISTS jobs")

        # clean db table
        await q.setup()

    @pytest.fixture
    def queue(self, event_loop):
        queue = joblib.JobQueue()
        event_loop.run_until_complete(self.queue_helper(queue))
        return queue

    @pytest.fixture
    def payload(self):
        return {
            "key": "value",
            "dict": {
                "foo": 1,
                "bar": "baz"
            }
        }

    @pytest.mark.asyncio
    async def test_request_and_acquire(self, queue, payload):
        # request should succeed
        await queue.request(jobtype="testing", payload=payload)
        # acquire should return same payload
        assert payload == (await queue.acquire(jobtype="testing"))[1]
        # there should not be another job
        assert None == (await queue.acquire(jobtype="testing"))[1]

    @pytest.mark.asyncio
    async def test_cleanup(self, queue, payload):
        await queue.request(jobtype="testing", payload=payload)
        # mark job as processing
        jobid_1, payload_1, _ = await queue.acquire(jobtype="testing")
        assert payload_1 == payload
        await queue.cleanup()
        # same job should be available again
        jobid_2, payload_2, _ = await queue.acquire(jobtype="testing")
        assert jobid_1 == jobid_2 and payload_1 == payload_2

    @pytest.mark.asyncio
    async def test_priority(self, queue, payload):
        await queue.request(jobtype="testing", payload=payload, priority=9)
        assert 9 == (await queue.acquire(jobtype="testing"))[2]

    @pytest.mark.asyncio
    async def test_fail(self, queue, payload):
        err = "testing errors"
        await queue.request(jobtype="testing", payload=payload)
        jobid, _, _ = await queue.acquire(jobtype="testing")
        await queue.fail(jobid, err)
        async with queue._pool.acquire() as con:
            jid, pl = await con.fetchrow(
                "SELECT id, payload FROM jobs WHERE status='failed'")
            assert jid == jobid
            payload["error"] = err
            assert json.loads(pl) == payload

    @pytest.mark.asyncio
    async def test_finish(self, queue, payload):
        await queue.request(jobtype="testing", payload=payload)
        jobid, _, _ = await queue.acquire(jobtype="testing")
        await queue.finish(jobid)
        # job should not be available again
        assert None == (await queue.acquire(jobtype="testing"))[1]
