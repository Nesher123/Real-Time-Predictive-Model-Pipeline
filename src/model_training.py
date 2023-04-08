"""
This module is responsible for training the model and saving it to disk.
"""
import pandas as pd
import os
import pickle
import json
import logging
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from data_processor import preprocess_data


def save_model(model, model_name: str) -> None:
    """
    Save the model to disk

    :param model: the model to save
    :param model_name: the name of the model
    :return: None
    """
    try:
        model_dir = 'model'

        # create the model_dir folder if it doesn't exist
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)

        model_filename = os.path.join(model_dir, model_name)

        # save the model to disk
        with open(model_filename, 'wb') as f:
            pickle.dump(model, f)

        logging.info(f'Model saved to {model_filename}')
    except Exception as e:
        logging.error(f'Error occurred while saving the model: {e}')


def train_and_save_model(df: pd.DataFrame) -> None:
    """
    Take the generated data, split it into training and testing sets, and train a RandomForestClassifier.
    The trained model is then saved as 'model.pkl' for later use.
    Model accuracy is calculated on the test set and printed as output.

    :param df: The generated data
    :return: None
    """

    random_state = 42

    try:
        # Split data into features and target variable
        X = df.drop('purchase', axis=1)
        y = df['purchase']

        # Split the dataset into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_state)

        # Train the RandomForestClassifier
        clf = RandomForestClassifier(random_state=random_state)
        clf.fit(X_train, y_train)

        save_model(clf, 'model.pkl')

        # Evaluate model accuracy on the test set
        accuracy = clf.score(X_test, y_test)
        print(f'Model accuracy: {accuracy:.2f}')
    except Exception as e:
        logging.error(f'Error occurred while training and saving model: {e}')


if __name__ == '__main__':
    try:
        config = json.load(open('../config.json'))
        synthetic_data = pd.read_csv('data/synthetic_data.csv')
        preprocessed_data = preprocess_data(synthetic_data)
        train_and_save_model(preprocessed_data)
    except Exception as e:
        logging.error(f'Error occurred in train_model.py: {e}')
