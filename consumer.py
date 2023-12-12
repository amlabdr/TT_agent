from confluent_kafka import Consumer, KafkaError
import json

END_MARKER = "__END_MARKER__"

def consume_data(consumer, topic):
    consumer.subscribe([topic])

    data_batches = {}  # To store batches for each key

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        key = msg.key().decode('utf-8')
        values = json.loads(msg.value().decode('utf-8'))

        # Check if the message is the end marker
        if END_MARKER in values:
            # Remove the end marker from the data
            values.remove(END_MARKER)
            data_batches[key].extend(values)
            # Yield the complete data for this key
            yield key, data_batches.pop(key, [])

        # Check if the key is already in data_batches
        if key not in data_batches:
            data_batches[key] = []

        # Append the batch values to the list
        data_batches[key].extend(values)

if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers
    group_id = 'a'  # Replace with your Kafka consumer group ID

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
    }

    # Choose the channel/topic you want to consume from
    channel_number_to_consume = 2
    topic_to_consume = f'channel_{channel_number_to_consume}'

    consumer = Consumer(consumer_config)

    try:
        for key, complete_data in consume_data(consumer, topic_to_consume):
            print(f'Received complete data for key {key}: with length {len(complete_data)}')
    except KeyboardInterrupt:
        consumer.close()
