import os
import pandas as pd
from flask import Flask, jsonify
import json
import pickle
from src.data_generator import generate_synthetic_data
from src.kafka_mock import MockKafkaConsumer, produce_synthetic_data, MockKafkaProducer
from src.mysql_mock import MockMySQLConnection
from src.data_processor import preprocess_data

app = Flask(__name__)
synthetic_data = generate_synthetic_data()  # pd.read_csv("src/synthetic_data.csv")

# Load the trained model
with open("src/model/model.pkl", "rb") as f:
    model = pickle.load(f)

# Set up mock Kafka consumer
kafka_producer = MockKafkaProducer()
kafka_consumer = MockKafkaConsumer(kafka_producer)

# Set up mock MySQL connection - insert the offline features into the MySQL database
mysql_conn = MockMySQLConnection(synthetic_data[["user_id", "past_purchase_amounts", "mean_purchase"]])

# Produce synthetic data for Kafka - simulate real-time features from Kafka
produce_synthetic_data(kafka_producer, synthetic_data[["user_id", "timestamp", "last_three_pages_visited"]])


# Set up the API endpoint - return the prediction for each user in real-time
@app.route("/predict", methods=["GET"])
def predict():
    """
    Get the real-time data from Kafka, preprocess the data, and make a prediction
    :return: the prediction for each user
    """

    # Get real-time data from Kafka
    message = kafka_consumer.poll()

    if message is None:
        return jsonify({"error": "No data available"}), 404

    # Extract features from the real-time data
    topic, value = message
    user_data = json.loads(value)

    # Get mean purchase from MySQL
    mean_purchase = mysql_conn.get_mean_purchase(user_data["user_id"])

    # Preprocess the data by calling the function in data_processor.py
    processed_data = preprocess_data(pd.DataFrame([user_data]))
    processed_data["mean_purchase"] = mean_purchase

    # Make a prediction
    probability = model.predict_proba(processed_data)[:, 1][0]

    return jsonify({"user_id": user_data["user_id"], "probability": probability})


if __name__ == "__main__":
    # Run the app
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
