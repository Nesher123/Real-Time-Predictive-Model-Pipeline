"""
This module contains the code for the producer.
The producer is responsible for producing the data to the Kafka topic.
It is a simple script that reads the data from a CSV file and sends it to the Kafka topic.
"""
import json
import logging
import asyncio
import pandas as pd
from aiokafka import AIOKafkaProducer


class KafkaProducer:
    def __init__(self, data: pd.DataFrame, topic: str, bootstrap_servers: str):
        self.data = data
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    async def produce_data(self, producer: AIOKafkaProducer) -> None:
        for _, i in self.data.iterrows():
            data = None

            try:
                data = {
                    'user_id': i['user_id'],
                    'timestamp': str(i['timestamp']),
                    'last_three_pages_visited': i['last_three_pages_visited']
                }
            except KeyError:
                logging.error('No feature found in message')
            except TypeError:
                logging.error('Message value is not a dictionary')
            except Exception as e:
                logging.error(f'Error occurred while processing messages: {e}')

            if not data:
                # skip message if no data is available or if an error occurred
                continue

            try:
                await producer.send_and_wait(self.topic, json.dumps(data).encode('utf-8'))
                logging.info(f'Sent data: {data} to topic: {self.topic}')
            except Exception as e:
                logging.error(f'Error occurred while sending data: {e}')
            finally:
                # sleep for 1 second to avoid overloading the Kafka broker
                await asyncio.sleep(1)

    async def start(self) -> None:
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await producer.start()
        try:
            await self.produce_data(producer)
        finally:
            await producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config = json.load(open('config.json'))
    synthetic_data = pd.read_csv(config['DATA_PATH'])
    producer = KafkaProducer(synthetic_data, config['INPUT_TOPIC'], config['KAFKA_BOOTSTRAP_SERVERS'])
    asyncio.run(producer.start())
