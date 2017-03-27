#!/usr/bin/python3

import json
import logging
import pika


class JobQueue(object):
    def __init__(self):
        self._qcon = None
        self._qchan = None

    def connect(self, **args):
        """Connect to RabbitMQ."""
        logging.info("connecting to broker")
        self._qcon = pika.BlockingConnection(pika.ConnectionParameters(
            **args))
        self._qchan = self._qcon.channel()

    def request(self, queue, payload):
        """Create a new job and return its id."""
        body = json.dumps(payload)
        self._qchan.queue_declare(queue=queue, durable=True)
        self._qchan.basic_publish(exchange="",
                                  routing_key=queue,
                                  body=body,
                                  properties=pika.BasicProperties(
                                      delivery_mode = 2
                                  ))

class JobFailed(Exception):
    pass


class Worker(JobQueue):
    """Abstract service worker class."""
    def __init__(self, jobtype):
        super().__init__()
        self._queue = jobtype
        self._up = False
        self._notifq = []

    def setup(self):
        # override
        pass

    def work(self, payload):
        # override
        pass

    def commit(self, failed):
        # override
        pass

    def request(self, queue, payload, now=True):
        if now:
            super().request(queue, payload)
        else:
            # send after COMMIT
            self._notifq.append((queue, payload))

    def run(self):
        self._qchan.queue_declare(queue=self._queue, durable=True)
        self._qchan.basic_qos(prefetch_count=1)
        for msg in self._qchan.consume(queue=self._queue,
                                       inactivity_timeout=1):
            # TODO force commit after n, c+=1
            if msg is None:
                # idling
                self.commit(False)
                # send notifs dependant on COMMIT
                for notif in self._notifq:
                    self.request(*notif)
                self._notifq = []
                continue

            method, properties, body = msg
            # run a job
            payload = json.loads(body)
            try:
                self.work(payload)
            except JobFailed as err:
                pass  # TODO
            self._qchan.basic_ack(delivery_tag=method.delivery_tag)
