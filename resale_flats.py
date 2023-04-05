import requests
import numpy as np
import pandas as pd
from datetime import datetime, MINYEAR, MAXYEAR
from IPython.display import display


class ResaleFlats:
    """
    Resale flat prices data created from data.gov.sg API

    Attributes
    ----------
    original_data: pd.DataFrame
        Stores the fetched CSV data

        * Columns:
            - town
            - flat_type
            - flat_model
            - floor_area_sqm
            - street_name
            - resale_price
            - month
            - remaining_lease
            - lease_commence_date
            - storey_range
            - block
            - price_per_sqm

    df: pd.DataFrame
        Working dataframe that is modified when filtered

    Methods
    -------
    fetch_data():
        Fetches resale flat prices data

    reset():
        Resets changes made to the `self.df`

    get_towns():
        Gets the list of unique town locales

    filter_by_town(towns):
        Filters data from the selected towns

    filter_by_flat_type(flat_types):
        Filters data from the selected flat types

    filter_by_time(start, end):
        Filters data within the time range

    show(n):
        Displays n rows of the dataframe in Jupyter Notebook
    """

    def __init__(self) -> None:
        self.original_data = self.fetch_data()
        self.df = self.original_data

    @staticmethod
    def fetch_data() -> pd.DataFrame:
        """
        Fetches resale flat prices data from data.gov.sg API
        """

        # data.gov.sg API batches CSVs into different time frames
        # current as of Apr 2023
        base_url = "https://data.gov.sg/api/action/datastore_search"
        resource_ids = [
            "f1765b54-a209-4718-8d38-a39237f502b3",  # Jan 2017 - Present
            "1b702208-44bf-4829-b620-4615ee19b57c",  # Jan 2015 - Dec 2016
            "83b2fc37-ce8c-4df4-968b-370fd818138b",  # Mar 2012 - Dec 2014
            "8c00bf08-9124-479e-aeca-7cc411d884c4",  # Jan 2000 - Feb 2012
            "adbbddd3-30e2-445f-a123-29bee150a6fe",  # Jan 1990 - Dec 1999
        ]

        # arbitrarily big limit so all entries are fetched
        # limit = 100
        limit = 1_000_000_000

        dfs = []
        for resource_id in resource_ids:
            data = requests.get(
                f"{base_url}?resource_id={resource_id}&limit={limit}")
            fetched_df = pd.json_normalize(
                data.json()["result"], record_path="records")
            dfs.append(fetched_df)

        # combine all 5 CSVs into one big dataframe
        df = pd.concat(dfs)

        # additional data
        df["resale_price"] = pd.to_numeric(df["resale_price"])
        df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"])
        df["price_per_sqm"] = (df["resale_price"] /
                               df["floor_area_sqm"]).round(2)
        df["date"] = pd.to_datetime(df["month"])

        return df

    def reset(self) -> None:
        """
        Resets data to the original dataframe
        """
        self.df = self.original_data

    def get_towns(self) -> np.ndarray:
        """
        Gets a list of town locales
        """
        return self.df["town"].unique()

    def filter_by_town(self, towns: list[str]) -> pd.DataFrame:
        """
        Returns the dataframe with data from the selected towns

        Parameters
        ----------
        * towns: list[str]
            Specifies which towns to include
        """
        self.df = self.df[self.df["town"].isin(towns)]
        return self.df

    def filter_by_flat_type(self, flat_types: list[str]) -> pd.DataFrame:
        """
        Returns the dataframe with data from the selected flat types

        Parameters
        ----------
        * flat_types: list[str]
            Specifies which flat types to include
        """
        self.df = self.df[self.df["flat_type"].isin(flat_types)]
        return self.df

    def filter_by_time(
        self,
        start: datetime = None,
        end: datetime = None
    ) -> pd.DataFrame:
        """
        Returns a dataframe with data from the selected time range

        Parameters
        ----------
        * start: datetime
            Specifies the earliest time to include (default is none)
        * end: datetime
            Specifies the latest time to include (default is none)
        """

        # if start is not specified, select all entries before end
        if start == None:
            start = datetime(MINYEAR, 1, 1)

        # if end is not specified, select all entries after start
        if end == None:
            end = datetime(MAXYEAR, 12, 31)

        self.df = self.df[(self.df["date"] >= start) &
                          (self.df["date"] <= end)]
        return self.df

    def show(self, n: int = 5) -> None:
        """ 
        Shows the first `n` rows of `self.df`

        Parameters
        ----------
        * n: int
            Specifies the number of rows to show (default is 5)
        """
        display(self.df.head(n))
