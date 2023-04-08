"""
This module contains the code for the consumer.
The consumer is responsible for consuming the predictions from the Kafka topic.
It is a simple script that logs the predictions to the console.
"""
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer


async def consume_predictions(consumer: AIOKafkaConsumer) -> None:
    async for message in consumer:
        try:
            logging.info(
                f'Received user_id: {message.value["user_id"]}, Received prediction: {message.value["prediction"]}')
        except KeyError:
            logging.error('No prediction found in message')
        except TypeError:
            logging.error('Message value is not a dictionary')
        except Exception as e:
            logging.error(f'Error occurred while consuming predictions: {e}')


async def main() -> None:
    consumer = AIOKafkaConsumer(
        config['OUTPUT_TOPIC'],
        bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    try:
        await consume_predictions(consumer)
    finally:
        await consumer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config = json.load(open('config.json'))
    asyncio.run(main())
