import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from data_generator import generate_synthetic_data
from data_processor import preprocess_data
from utils import save_model


def train_and_save_model(df: pd.DataFrame) -> None:
    """
    Take the generated data, split it into training and testing sets, and train a RandomForestClassifier.
    The trained model is then saved as 'model.pkl' for later use.
    Model accuracy is calculated on the test set and printed as output.

    :param df: The generated data
    :return: None
    """

    # Split data into features and target variable
    X = df.drop('purchase', axis=1)
    y = df['purchase']

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the RandomForestClassifier
    clf = RandomForestClassifier(random_state=42)
    clf.fit(X_train, y_train)

    save_model(clf, 'model.pkl')

    # Evaluate model accuracy on the test set
    accuracy = clf.score(X_test, y_test)
    print(f'Model accuracy: {accuracy:.2f}')


if __name__ == '__main__':
    synthetic_data = generate_synthetic_data()
    preprocessed_data = preprocess_data(synthetic_data)
    train_and_save_model(preprocessed_data)
