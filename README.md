# Schooling

A simple Python wrapper around Redis streams.

## Installation

### Requirements

Add the following line to `requirements.txt`
```bash
-e git+ssh://git@github.com/saleswhale/schooling-py.git#egg=schooling
```

Then do
```bash
pip install -r requirements.txt
```

### From Git
Install via
```bash
pip install git+https://github.com/saleswhale/schooling-py.git
```

or

```bash
pip install git+ssh://git@github.com/saleswhale/schooling-py.git
```

### From source
Navigate into the root directory `schooling-py` and run
```bash
python setup.py install
```

## Download

Download the archive by running
```bash
mkdir -p vendor
pip download --no-deps -d ./vendor -e git+ssh://git@github.com/saleswhale/schooling-py.git#schooling
```

Then install from path.

## Usage Guide

To test out the library, do
``` py
import random
import threading
import time

from schooling import Consumer, Producer, Processor

# A function which fails from time to time
def test_function(arg):
    if random.random() > 0.5:
        raise Exception('The odds are against you.')
    return ':)'

def publish_messages(p):
    for i in range(100):
        p.publish({'id': i})

def process_messages(c):
    while True:
        c.process()
        time.sleep(random.randint(1, 5))

# If redis_url is not provided, it defaults to 'redis://localhost:6379/1'
producer = Producer('test_topic')
consumer_1 = Consumer('test_topic',
                      'test_group',
                      'consumer_1',
                      Processor(test_function))
consumer_2 = Consumer('test_topic',
                      'test_group',
                      'consumer_2',
                      Processor(test_function))

# Spawning and joining threads
tp = threading.Thread(target=publish_messages, args=(producer,))
tc1 = threading.Thread(target=process_messages, args=(consumer_1,))
tc2 = threading.Thread(target=process_messages, args=(consumer_2,))
tp.start()
tc1.start()
tc2.start()
tp.join()
tc1.join(timeout=1000)
tc2.join(timeout=1000)
```

## References

1. [Ruby implementation](https://github.com/saleswhale/schooling_rb)
2. [Introducton to Redis Streams](https://redis.io/topics/streams-intro)
3. [Introduction to Redis Streams with Python](https://charlesleifer.com/blog/redis-streams-with-python/)
