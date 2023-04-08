"""
Real-time Prediction Service
============================
A microservice system that:
- Retrieves real-time data from a message broker (Apache Kafka).
- Extracts features from the real-time data.
- Extract features from an offline store (mock MySQL instance).
- Implements a data processing pipeline to clean and preprocess the data.
- Makes a prediction and returns the estimated probability.
"""
import pandas as pd
import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.data_processor import preprocess_data
from src.mysql_mock import MockMySQLConnection
from src.prediction_model import PredictionModel

# When no purchase-history data is available for a user,
# the generic mean purchase is used in order to enable some reasonable prediction.
COLD_START_MEAN_PURCHASE = 0.2


class PredictionPipeline:
    def __init__(self):
        try:
            self.consumer = AIOKafkaConsumer(
                config['INPUT_TOPIC'],
                bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            self.producer = AIOKafkaProducer(
                bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'],
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
            )

            self.connection = MockMySQLConnection(pd.read_csv(config['DATA_PATH']))
            self.model = PredictionModel(config['MODEL_PATH'])
        except Exception as e:
            logging.error(f'Error occurred while initializing the prediction pipeline: {e}')

    async def process_messages(self) -> None:
        async for message in self.consumer:
            try:
                user_data = message.value
                user_id = user_data['user_id']

                # Get mean purchase from MySQL
                mean_purchase = await asyncio.to_thread(self.connection.get_mean_purchase, user_id)

                if mean_purchase is None:
                    # when no data is available for a user, we still want to allow the user to get a prediction
                    mean_purchase = COLD_START_MEAN_PURCHASE  # use the generic mean purchase
                    # another option is to return an error:
                    # raise KeyError('No data available')

                # Preprocess the data by calling the function in data_processor.py
                processed_data = preprocess_data(pd.DataFrame([user_data]))
                processed_data['mean_purchase'] = mean_purchase
                prediction = self.model.predict(processed_data)
                await self.producer.send_and_wait(config['OUTPUT_TOPIC'],
                                                  {'user_id': user_id, 'prediction': prediction})
                # print user id and prediction in nice formatted way
                logging.info(f'User {user_id} - Prediction: {prediction}. Sent to topic: output_topic')
            except KeyError:
                logging.error('No feature found in message')
            except TypeError:
                logging.error('Message value is not a dictionary')
            except Exception as e:
                logging.error(f'Error occurred while processing messages: {e}')

    async def start(self):
        logging.info('Prediction pipeline starting')
        await self.consumer.start()
        await self.producer.start()
        await self.process_messages()
        logging.info('Prediction pipeline started')

    async def stop(self):
        logging.info('Prediction pipeline stopping')
        await self.consumer.stop()
        await self.producer.stop()
        logging.info('Prediction pipeline stopped')


async def run_pipeline():
    try:
        pipeline = PredictionPipeline()
        await pipeline.start()
        await pipeline.stop()
    except Exception as e:
        logging.error(f'Error in prediction pipeline: {e}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        config = json.load(open('config.json'))
        asyncio.run(run_pipeline())
    except Exception as e:
        logging.error(f'Error occurred while starting prediction pipeline: {e}')
