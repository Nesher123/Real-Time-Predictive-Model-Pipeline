import pandas as pd
import logging

# map pages to numerical values
PAGES = {
    'home': 0,
    'products': 1,
    'cart': 2,
    'checkout': 3
}

FEATURES_FOR_MODEL = ['page_1', 'page_2', 'page_3', 'mean_purchase', 'purchase']


def _map_pages(data: pd.DataFrame) -> pd.DataFrame:
    """
    Map each value in the dataframe to the corresponding page
    :param data: dataframe with the pages
    :return: dataframe with mapped values
    """
    try:
        return data.applymap(lambda col: PAGES[col])
    except Exception as e:
        logging.error(f'Error occurred while mapping pages: {e}')


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the data for the model
    :param data: dataframe with the data
    :return: dataframe with the preprocessed data
    """
    try:
        # split the last_three_pages_visited into separate columns
        last_three_pages_visited_split = data['last_three_pages_visited'].str.split('/', expand=True)
        last_three_pages_visited_split.columns = ['page_1', 'page_2', 'page_3']

        # map pages to numerical values
        last_three_pages_visited_split = _map_pages(last_three_pages_visited_split)

        # add the new columns to the dataframe
        data = pd.concat([data, last_three_pages_visited_split], axis=1)

        return data[[c for c in FEATURES_FOR_MODEL if c in data.columns]]
    except Exception as e:
        logging.error(f'Error occurred while preprocessing data: {e}')


if __name__ == '__main__':
    # for testing purposes
    test_data = pd.DataFrame([['home/cart/checkout', 1, 0], ['home/cart/products', 1, 0]],
                             columns=['last_three_pages_visited', 'mean_purchase', 'purchase'])
    print(preprocess_data(test_data))
