import json
import logging
import os
import pickle
import pandas as pd
import asyncio
from fastapi.responses import JSONResponse, HTMLResponse
from .utils import get_project_root
from .kafka_mock import MockKafkaProducer, MockKafkaConsumer, produce_synthetic_data
from .mysql_mock import MockMySQLConnection
from .data_processor import preprocess_data
from .data_generator import generate_synthetic_data

# Load the trained model and synthetic_data in the services file or another appropriate file
synthetic_data = generate_synthetic_data()  # pd.read_csv('src/synthetic_data.csv')
model_file_path = os.path.join(get_project_root(), 'src', 'model', 'model.pkl')

with open(model_file_path, 'rb') as f:
    model = pickle.load(f)

# Initialize the MockKafkaProducer, MockKafkaConsumer, and MockMySQLConnection
kafka_producer = MockKafkaProducer()
kafka_consumer = MockKafkaConsumer(kafka_producer)
mysql_conn = MockMySQLConnection(synthetic_data[['user_id', 'past_purchase_amounts', 'mean_purchase']])

logging.info('Connected to database')

# Produce synthetic data for Kafka - simulate real-time features from Kafka
produce_synthetic_data(kafka_producer, synthetic_data[["user_id", "timestamp", "last_three_pages_visited"]])


async def root() -> HTMLResponse or JSONResponse:
    """
    Return the HTML file for the web app
    :return: HTMLResponse object containing the HTML file for the web app or a JSONResponse object containing the error
    message
    """

    try:
        # get HTML from file
        with open('templates/index.html', 'r') as f:
            html_content = f.read()
    except FileNotFoundError:
        # also add logging error
        err_msg = 'No html file found'
        logging.error(err_msg)
        return JSONResponse(content={'error': err_msg}, status_code=404)

    return HTMLResponse(content=html_content, status_code=200)


async def predict() -> JSONResponse:
    """
    Get the real-time data from Kafka, preprocess the data, and make a prediction
    :return: JSONResponse object containing the user_id and the probability of purchase
    """

    # Get real-time data from Kafka
    message = await asyncio.to_thread(kafka_consumer.poll)

    if message is None:
        return JSONResponse(content={'error': 'No data available'}, status_code=404)

    # Extract features from the real-time data
    topic, value = message
    user_data = json.loads(value)

    # Get mean purchase from MySQL
    mean_purchase = await asyncio.to_thread(mysql_conn.get_mean_purchase, user_data['user_id'])

    # Preprocess the data by calling the function in data_processor.py
    processed_data = preprocess_data(pd.DataFrame([user_data]))
    processed_data['mean_purchase'] = mean_purchase

    # Make a prediction
    probability = model.predict_proba(processed_data)[:, 1][0]

    return JSONResponse(content={'user_id': user_data['user_id'], 'probability': probability}, status_code=200)
