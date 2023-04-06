import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


def generate_synthetic_data(num_samples: int = 1000) -> pd.DataFrame:
    np.random.seed(42)

    # Generate random user_ids
    user_ids = np.random.randint(0, 9999, size=num_samples)

    # Generate random timestamps for browsing events
    now = datetime.now()
    browsing_timestamps = [now - timedelta(minutes=random.randint(1, 10000)) for _ in range(num_samples)]

    # Generate random page visits (3 pages per user)
    pages = ['home', 'products', 'cart', 'checkout']
    last_three_pages_visited = ['/'.join(random.sample(pages, 3)) for _ in range(num_samples)]

    # Generate random past purchase amounts (3 purchases per user)
    past_purchase_amounts = [np.round(np.random.randint(5, 100, 3)).tolist() for _ in range(num_samples)]

    # get the mean of the last three purchases
    past_purchase_amounts_mean = [np.mean(past_purchase_amounts[i]) for i in range(num_samples)]

    # Generate binary purchase outcome (1 for purchase, 0 for no purchase)
    purchase_outcome = np.random.choice([0, 1], size=num_samples, p=[0.7, 0.3])

    # Create a DataFrame
    data = pd.DataFrame({
        'user_id': user_ids,
        'timestamp': browsing_timestamps,
        'last_three_pages_visited': last_three_pages_visited,
        'past_purchase_amounts': past_purchase_amounts,
        'mean_purchase': past_purchase_amounts_mean,
        'purchase': purchase_outcome
    })

    return data


if __name__ == '__main__':
    synthetic_data = generate_synthetic_data()
    print(synthetic_data.head())
    # save csv
    synthetic_data.to_csv('synthetic_data.csv', index=False)
