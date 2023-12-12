import numpy as np
import yaml
import concurrent.futures
from functools import partial

import time
import random
import json

#import kafka
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
END_MARKER = "__END_MARKER__"

# Function to read  config.yaml
def read_config_file():
    with open('config.yaml', 'r') as file:
        config_data = yaml.safe_load(file)
        channels = config_data.get('channels')
        WR_channel =  config_data.get('WR_channel')
    return channels, WR_channel

def generate_data():
    data = {}
    while True:
        current_second = int(time.time())
        timestamp_list = [current_second + _*1000 for _ in range(1000000)]
        data = {current_second: timestamp_list}
        yield data
        time.sleep(1)
    

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_topic_if_not_exists(admin_client, topic):
    topics = admin_client.list_topics().topics
    if topic not in topics:
        admin_client.create_topics([
            NewTopic(topic=topic, num_partitions=1, replication_factor=1)
        ])

def process_data(producer, admin_client, topic_prefix, batch_size,ch):
    topic = f'{topic_prefix}{ch}'
    create_topic_if_not_exists(admin_client, topic)
    data_generator = generate_data()
    while True:
        current_data = next(data_generator)
        key, values = next(iter(current_data.items()))
        # PRODUCE TO KAFKA SERVER
        for i in range(0, len(values), batch_size):
            batch_values = values[i:i+batch_size]
            # Check if this is the last segment
            is_last_segment = (i + batch_size) >= len(values)
            # Add the END_MARKER to the last segment
            if is_last_segment:
                batch_values.append(END_MARKER)
            produce_data(producer, admin_client, topic_prefix, ch, key, batch_values)
        print(f"Channel {ch}: {'DONE'}")

def produce_data(producer, admin_client, topic_prefix, channel,  key, batch_values):
    topic = f'{topic_prefix}{channel}'
    create_topic_if_not_exists(admin_client, topic)
    try:
        producer.produce(topic, key=str(key), value=json.dumps(batch_values).encode('utf-8'), callback=delivery_report)
    except Exception as e:
        print(f"Channel {channel}: Error producing message - {e}")
    producer.poll(0)  # Trigger delivery reports

if __name__ == '__main__':
    channels, WR_channel= read_config_file()
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers
    topic_prefix = 'channel_'  # Prefix for your topics

    producer_config = {
        'bootstrap.servers': bootstrap_servers
    }
    batch_size = 10000

    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    producer = Producer(producer_config)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Use functools.partial to fix additional parameters of process_data
        partial_process_data = partial(process_data, producer, admin_client, topic_prefix,batch_size)
        
        # Submit each channel processing to the executor
        futures = [executor.submit(partial_process_data, ch) for ch in channels]
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)