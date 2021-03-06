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

    def __init__(self,
                 topic,
                 redis=None,
                 redis_url='redis://localhost:6379/0',
                 logger=None):
        self.topic = topic
        self.logger = logger or StreamLogger(self.topic)
        if redis is None:
            self.redis_url = redis_url
            self.redis = Redis.from_url(redis_url)
            self.logger.info(f'Connected to {redis_url}.')
        else:
            self.redis = redis
            self.logger.info(f'Connected to Redis.')
        self.logger.info(f'Listening for {self.topic}.')

    def info(self):
        self.logger.info(f'Obtaining stream info for {self.topic}.')
        stream_info = self.redis.xinfo_stream(self.topic)
        groups_info = self.redis.xinfo_groups(self.topic)
        stream_info.update({'groups_info': groups_info})
        return stream_info

    def count(self):
        self.logger.info(f'Checking number of events in {self.topic}.')
        return self.redis.xlen(self.topic)

    def list_groups(self):
        self.logger.info(f'Retrieving consumer groups in {self.topic}.')
        return self.redis.xinfo_groups(self.topic)
    
    def delete(self, *message_ids):
        self.logger.info(f'Deleting messages from {self.topic}.')
        deleted = self.redis.xdel(self.topic, message_ids)
        self.logger.info(f'Deleted {deleted} messages from {self.topic}.')
        return deleted

    def trim(self, maxlen):
        return self.redis.xtrim(self.topic, maxlen)


class Consumer(StreamIO):
    """
    Wrapper for Redis consumer.
    """

    def __init__(self,
                 topic,
                 group,
                 consumer,
                 processor=None,
                 redis=None,
                 redis_url='redis://localhost:6379/0',
                 batch_size=BATCH_SIZE,
                 block=DEFAULT_BLOCK,
                 backoff=ExponentialBackoff,
                 logger=None):
        super().__init__(topic, redis, redis_url, logger)
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
        return self.redis.xgroup_create(self.topic,
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
                    self.redis.xack(self.topic, self.group, event_id)
                    self.logger.info(f'{event_id} succeeded.')
                except Exception as e:
                    self.logger.error(f'Failed {event_id} due to {e}.')

    def process(self, processor=None):
        self.processor = processor or self.processor
        if self.processor is None:
            self.logger.error('No processing function defined.')
            raise Exception('Processing function is invalid.')
        self.__process_failed_events()
        self.__process_unseen_events()

    def __process_unseen_events(self):
        self.logger.debug(f'Processing unseen events in {self.topic}.')
        streams = { self.topic: '>' }
        unseen_events = self.redis.xreadgroup(self.group,
                                              self.consumer,
                                              streams,
                                              block=self.block)
        if len(unseen_events):
            self.process_event(*unseen_events[0][1])
        self.logger.debug('No new events.')

    def __process_failed_events(self):
        self.logger.debug(f'Processing failed events in {self.topic}.')
        pending_events = self.redis.xpending_range(self.topic,
                                                   self.group,
                                                   '-',
                                                   '+',
                                                   self.batch_size)
        for event in pending_events:
            event_id = event['message_id'].decode('utf-8')
            deliveries = event['times_delivered']
            timeout = self.backoff.timeout_ms(deliveries)
            failed_events = self.redis.xclaim(self.topic,
                                              self.group,
                                              self.consumer,
                                              timeout,
                                              [event_id])
            if len(failed_events):
                self.process_event(*failed_events)
        self.logger.debug('No failed events.')


class Producer(StreamIO):
    """
    Wrapper for Redis producer.
    """

    def __init__(self,
                 topic,
                 redis=None,
                 redis_url='redis://localhost:6379/0',
                 cap=DEFAULT_CAP,
                 logger=None):
        super().__init__(topic, redis, redis_url, logger)
        self.cap = cap

    def publish(self, *messages):
        published = [self.redis.xadd(self.topic,
                                     {'json': json.dumps(msg)},
                                     maxlen=self.cap) \
            for msg in messages]
        self.logger.info(f'Published messages {published[0]} to ' \
            f'{published[-1]} to {self.topic}.')
        return published

