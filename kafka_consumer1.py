from confluent_kafka import Consumer, KafkaError


settings = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer_group',
    'client.id': 'client_1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)


c.subscribe(topics=['test_topic'])


try:
    while True:
        message = c.poll()
        if message is None:
            continue

        elif not message.error():
            print('Message received: {}'.format(message.value()))
        elif message.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {}/{}'.format(message.topic(), message.partition()))
        else:
            print('Error occurred: {}'.format(message.error().str()))
except KeyboardInterrupt:
    pass

finally:
    c.close()