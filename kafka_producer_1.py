from confluent_kafka import Producer
from typing import Any


# Callback function to display error
def acknowledged(err: Any, msg: Any) -> None:
    if err is not None:
        print('Failed to deliver message: {0}: {1}'.format(msg.value(), err.str))
    else:
        print('Message produced: {}'.format(msg.value()))


p = Producer({'bootstrap.servers': 'localhost:9092'})
try:
    for i in range(10000):
        p.produce(topic='test_topic', value='What ever -- {}'.format(i), callback=acknowledged)
        p.poll(timeout=0.5)
except KeyboardInterrupt:
    pass

ret_val = p.flush(timeout=30)
print(ret_val)
