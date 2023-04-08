"""
A class that loads a model and makes predictions.
This is used by the prediction service to make predictions.
"""
import pandas as pd
import pickle
import logging


class PredictionModel:
    def __init__(self, model_path: str):
        self.model = None
        self.load_model(model_path)

    def load_model(self, model_path: str) -> None:
        try:
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)
        except FileNotFoundError:
            logging.error('No model file found')
        except Exception as e:
            logging.error(f'Error occurred while loading the model: {e}')

    def predict(self, input_data: pd.DataFrame) -> float:
        try:
            return round(float(self.model.predict_proba(input_data)[:, 1]), 2)
        except Exception as e:
            logging.error(f'Error occurred while making predictions: {e}')
