import json
import time
import pandas as pd


class MockKafkaProducer:
    """This class is used to mock the Kafka producer"""

    def __init__(self):
        self.data = []

    def send(self, topic: str, value: str) -> None:
        """
        Send data to the producer.

        :param topic: the topic to send the data to
        :param value: the data to send
        :return: None
        """
        self.data.append((topic, value))


class MockKafkaConsumer:
    """This class is used to mock the Kafka consumer"""

    def __init__(self, producer: MockKafkaProducer):
        self.producer = producer

    def poll(self, timeout_sec: int = 1) -> str or None:
        """
        Get the data from the producer. If no data is available, wait for the specified timeout.

        :param timeout_sec: timeout in seconds
        :return: data from the producer or None if no data is available
        """
        time.sleep(timeout_sec)

        if self.producer.data:
            return self.producer.data.pop(0)

        return None


def produce_synthetic_data(producer: MockKafkaProducer, data: pd.DataFrame) -> None:
    """
    Produce synthetic data for Kafka.

    :param producer: the Kafka producer
    :param data: the data to produce
    :return: None
    """
    for _, row in data.iterrows():
        message = {
            "user_id": row["user_id"],
            "timestamp": str(row["timestamp"]),
            "last_three_pages_visited": row["last_three_pages_visited"]
        }
        producer.send("user_browsing_data", json.dumps(message))
