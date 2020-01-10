import json
import time
from unittest.mock import Mock

import pytest

import schooling
from schooling import Consumer, Producer, Processor
from schooling.stream import StreamIO, DEFAULT_CAP

class MockRedis(Mock):
    """
    Mock redis conn class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.last_count = {}
        self.streams = {}
        self.all_events = {}
        self.pending = {}
        self.acks = {}

    def xlen(self, key):
        if key in self.all_events.keys():
            return len(self.all_events[key])
        return 0

    def xinfo_stream(self, key):
        return self.streams[key]

    def xinfo_groups(self, key):
        return [v for k, v in self.streams[key].items()]

    def xgroup_create(self, key, group, id=0, mkstream=False):
        if mkstream:
            if key not in self.streams.keys():
                self.streams[key] = {}
                self.all_events[key] = {}
                self.pending[key] = {}
                self.last_count[key] = 0
        if group in self.streams[key]:
            raise Exception('Consumer group already exists.')
        self.streams[key][group] = {
            'last_id': -1,
            'name': group
        }
        return True

    def xadd(self, key, message, **kwargs):
        if key not in self.all_events.keys():
            self.streams[key] = {}
            self.all_events[key] = {}
            self.pending[key] = {}
            self.last_count[key] = 0
        message = {str.encode(k): v for k, v in message.items()}
        message['insert_time'] = time.time()
        message_id = str(self.last_count[key])
        self.all_events[key][message_id] = message
        self.last_count[key] += 1
        return message_id

    def xreadgroup(self, group, consumer, keys, **kwargs):
        filtered_events = []
        for key in keys.keys():
            stream_events = [(str.encode(k), v) \
                for k, v in self.all_events[key].items() \
                if int(k) > self.streams[key][group]['last_id']]
            filtered_events.extend(stream_events)
        max_id = self.streams[key][group]['last_id']
        if group not in self.pending[key].keys():
            self.pending[key][group] = {}
        for k, v in filtered_events:
            self.pending[key][group][k] = v
            self.pending[key][group][k]['consumer'] = consumer
            self.pending[key][group][k]['times_delivered'] = 1
            max_id = max(int(k), max_id)
        self.streams[key][group]['last_id'] = max_id
        return [[key, filtered_events]]

    def xpending_range(self, key, group, start, end, batch_size):
        if group in self.pending[key].keys():
            return [(k, v) for k, v in self.pending[key][group][:batch_size]]
        return []

    def xclaim(self, key, group, consumer, timeout, event_ids):
        current_time = time.time()
        claimed = []
        for k, v in self.pending[key][group].items():
            if k in event_ids:
                if v['insert_time'] < current_time - timeout:
                    self.pending[key][group][k]['consumer'] = consumer
                    self.pending[key][group][k]['insert_time'] = current_time
                    claimed.append((k, v))
        return claimed

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def mock_redis():
    return MockRedis()

@pytest.fixture
def streamio(mock_redis, mock_logger):
    return StreamIO('test_topic', redis=mock_redis, logger=mock_logger)

def test_streamio_init(streamio):
    assert(streamio.topic == 'test_topic')
    assert(isinstance(streamio.redis, MockRedis))

def test_stream_init_wurl(monkeypatch):
    monkeypatch.setattr(schooling.stream, 'Redis', MockRedis)
    streamio = StreamIO('test_topic')
    assert(streamio.topic == 'test_topic')
    assert(streamio.redis_url == 'redis://localhost:6379/0')
    assert(isinstance(streamio.redis, MockRedis))

def test_streamio_count(streamio):
    assert(streamio.count() == 0)

def test_streamio_info(streamio):
    with pytest.raises(Exception):
        streamio.info()

def test_streamio_list_groups(streamio):
    with pytest.raises(Exception):
        streamio.list_groups()

def test_streamio_trim(streamio):
    streamio.trim(1)
    streamio.redis.xtrim.assert_called_once_with(streamio.topic, 1)

@pytest.fixture
def producer(mock_redis, mock_logger):
    return Producer('test_topic', redis=mock_redis, logger=mock_logger)

def test_producer_init(producer):
    assert(producer.topic == 'test_topic')
    assert(producer.cap == DEFAULT_CAP)
    assert(isinstance(producer.redis, MockRedis))

def test_producer_publish_one(producer):
    assert(producer.count() == 0)
    producer.publish({'id': 'hello'})
    assert(producer.count() == 1)

def test_producer_info(producer):
    producer.publish({'id': 'hello'})
    assert( 'groups_info' in producer.info().keys())

def test_producer_publish_multiple(producer):
    assert(producer.count() == 0)
    events = [{'id': i} for i in range(10)]
    producer.publish(*events)
    assert(producer.count() == 10)

@pytest.fixture
def processor():
    def does_nothing(*args, **kwargs):
        pass
    return Processor(does_nothing)

@pytest.fixture
def error_processor():
    def raise_error(*args, **kwargs):
        raise Exception('Error!')
    return Processor(raise_error)

@pytest.fixture
def consumer(processor, mock_redis, mock_logger):
    return Consumer('test_topic',
                    'test_group',
                    'test_consumer',
                    processor,
                    redis=mock_redis,
                    logger=mock_logger)

def test_consumer_init(consumer):
    assert(consumer.topic == 'test_topic')
    assert(consumer.group == 'test_group')
    assert(consumer.consumer == 'test_consumer')
    assert(isinstance(consumer.processor, Processor))
    assert(isinstance(consumer.redis, MockRedis))

def test_consumer_info(consumer):
    assert(consumer.group in consumer.info().keys())
    assert('groups_info' in consumer.info().keys())

def test_consumer_create_group(consumer):
    assert('test_group' in consumer.list_groups())

def test_consumer_process_events(consumer):
    consumer.process_event((b'0', {b'json': json.dumps({'hello': 'world'})}))
    consumer.redis.xack.assert_called_once()

def test_consumer_process_events_errors(consumer, error_processor):
    consumer.processor = error_processor
    consumer.process_event((b'0', {b'json': json.dumps({'hello': 'world'})}))
    consumer.redis.xack.assert_not_called()
    consumer.logger.error.assert_called_once()

def test_consumer_process(producer, consumer):
    producer.publish({'test': 'message'})
    assert('test_group' in consumer.list_groups())
    consumer.process()
    consumer.logger.error.assert_not_called()
    consumer.redis.xack.assert_called_once()
