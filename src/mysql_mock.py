import pandas as pd


class MockMySQLConnection:
    """This class is used to mock the MySQL connection"""

    def __init__(self, data: pd.DataFrame):
        self.data = data

    def get_mean_purchase(self, user_id: int) -> float or None:
        """
        Get the mean purchase for the specified user.

        :param user_id: the user ID
        :return: the mean purchase or None if the user does not exist in DB
        """
        row = self.data.loc[self.data["user_id"] == user_id]  # "SELECT mean_purchase FROM data WHERE user_id = user_id"

        if row.empty:
            return None

        return float(row["mean_purchase"].values[0])
