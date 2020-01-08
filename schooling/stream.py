import json
import logging
import re
import sys
import time
from urllib.parse import urlparse

from redis import Redis

from schooling.backoff import ExponentialBackoff
from schooling.logger import StreamLogger

SECS = 1000
DEFAULT_BLOCK = 2 * SECS
BATCH_SIZE = 1000
DEFAULT_CAP = 100_000


class StreamIO:
    """
    Wrapper for Redis stream.
    """

    def __init__(self, redis_url, topic, logger=None):
        parsed_url = urlparse(redis_url)
        self.redis_url = redis_url
        self.redis_host = parsed_url.netloc.split(':')[0]
        self.redis_port = parsed_url.port
        self.redis_db = int(re.sub('[^0-9]', '', parsed_url.path or '0'))
        self.redis_conn = Redis(host=self.redis_host,
                                port=self.redis_port,
                                db=self.redis_db)
        self.topic = topic
        self.logger = logger or StreamLogger(self.topic)
        self.logger.info(f'Connected to {redis_url}.')
        self.logger.info(f'Listening for {self.topic}.')

    def info(self):
        self.logger.info(f'Obtaining stream info for {self.topic}.')
        stream_info = self.redis_conn.xinfo_stream(self.topic)
        groups_info = self.redis_conn.xinfo_groups(self.topic)
        stream_info.update({'groups_info': groups_info})
        return stream_info

    def count(self):
        self.logger.info(f'Checking number of events in {self.topic}.')
        return self.redis_conn.xlen(self.topic)

    def list_groups(self):
        self.logger.info(f'Retrieving consumer groups in {self.topic}.')
        return [group['name'] \
            for group in self.redis_conn.xinfo_groups(self.topic)]

    def trim(self, maxlen):
        return self.redis_conn.xtrim(self.topic, maxlen)


class Consumer(StreamIO):
    """
    Wrapper for Redis consumer.
    """

    def __init__(self,
                 topic,
                 group,
                 consumer,
                 processor,
                 redis_url='redis://localhost:6379/0',
                 batch_size=BATCH_SIZE,
                 block=DEFAULT_BLOCK,
                 backoff=ExponentialBackoff,
                 logger=None):
        super().__init__(redis_url, topic, logger)
        self.group = group
        self.consumer = consumer
        self.processor = processor
        self.batch_size = batch_size
        self.block = block
        self.backoff = backoff
        try:
            self.create_group()
        except Exception as e:
            self.logger.info(e)

    def create_group(self, last_id='$', mkstream=True):
        self.logger.info(f'Adding {self.group} to {self.topic}.')
        return self.redis_conn.xgroup_create(self.topic,
                                             self.group,
                                             id=last_id,
                                             mkstream=mkstream)

    def process_event(self, *events):
        for event in events:
            if event[0]:
                event_id = event[0].decode('utf-8')
                try:
                    self.logger.info(f'Processing {event_id}.')
                    self.processor(json.loads(event[1][b'json']))
                    self.redis_conn.xack(self.topic, self.group, event_id)
                    self.logger.info(f'{event_id} Success.')
                except Exception as e:
                    self.logger.error(f'Failed {event_id} due to {e}.')

    def process(self):
        self.__process_failed_events()
        self.__process_unseen_events()

    def __process_unseen_events(self):
        self.logger.info(f'Processing unseen events in {self.topic}.')
        streams = { self.topic: '>' }
        unseen_events = self.redis_conn.xreadgroup(self.group,
                                                   self.consumer,
                                                   streams,
                                                   block=self.block)
        if len(unseen_events):
            self.process_event(*unseen_events[0][1])
        self.logger.info('No new events.')

    def __process_failed_events(self):
        self.logger.info(f'Processing failed events in {self.topic}.')
        pending_events = self.redis_conn.xpending_range(self.topic,
                                                        self.group,
                                                        '-',
                                                        '+',
                                                        self.batch_size)
        for event in pending_events:
            event_id = event['message_id'].decode('utf-8')
            deliveries = event['times_delivered']
            timeout = self.backoff.timeout_ms(deliveries)
            failed_events = self.redis_conn.xclaim(self.topic,
                                                   self.group,
                                                   self.consumer,
                                                   timeout,
                                                   [event_id])
            if len(failed_events):
                self.process_event(*failed_events)
        self.logger.info('No failed events.')


class Producer(StreamIO):
    """
    Wrapper for Redis producer.
    """

    def __init__(self,
                 topic,
                 redis_url='redis://localhost:6379/0',
                 cap=DEFAULT_CAP,
                 logger=None):
        super().__init__(redis_url, topic, logger)
        self.cap = cap

    def publish(self, *messages):
        self.logger.info(f'Pushing message(s) to {self.topic}.')
        return [self.redis_conn.xadd(self.topic,
                                     {'json': json.dumps(msg)},
                                     maxlen=self.cap) \
            for msg in messages]

