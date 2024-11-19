import os
import re
from json import loads
from pathlib import Path
from typing import Any

import pandas as pd
from monzo_api_wrapper.utils.custom_logger import loggable


@loggable
def map_category_id(transactions_df: pd.DataFrame, categories_dict: dict) -> pd.DataFrame:
    """Map category names to category IDs using the provided dictionary.

    Args:
        transactions_df (pd.DataFrame): DataFrame containing transaction withs col 'category'.
        categories_dict (dict): Dictionary mapping category names to category IDs.

    Returns:
        pd.DataFrame: DataFrame with a new 'category_id' column.

    """
    transactions_df["category_id"] = transactions_df["category"].map(categories_dict)
    return transactions_df


@loggable
def map_asset_id(transactions_df: pd.DataFrame, assets_ids_dict: dict) -> pd.DataFrame:
    """Map source names to asset IDs and ensure IDs are integers.

    Args:
        transactions_df (pd.DataFrame): DataFrame containing transactions a 'source' column.
        assets_ids_dict (dict): Dictionary mapping source names to asset IDs.

    Returns:
        pd.DataFrame: DataFrame with a new 'asset_id' column as integers.

    """
    transactions_df["asset_id"] = transactions_df["source"].map(assets_ids_dict).astype(int)
    return transactions_df


@loggable
def format_date_column(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Format the 'date' column to 'YYYY-MM-DD' string format.

    Args:
        transactions_df (pd.DataFrame): DataFrame with a 'date' column in various formats.

    Returns:
        pd.DataFrame: DataFrame with 'date' column formatted as 'YYYY-MM-DD'.

    """
    transactions_df["date"] = pd.to_datetime(transactions_df["date"]).dt.strftime("%Y-%m-%d")
    return transactions_df


@loggable
def assign_payee_column(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Assign the 'description' column to a new 'payee' column.

    Args:
        transactions_df (pd.DataFrame): DataFrame of transactions with a 'description' column.

    Returns:
        pd.DataFrame: DataFrame with a new 'payee' column.

    """
    transactions_df["payee"] = transactions_df["description"]
    return transactions_df


@loggable
def replace_blank_with_none(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Replace blank strings with None.

    Args:
        transactions_df (pd.DataFrame): DataFrame with blank strings that need replacing.

    Returns:
        pd.DataFrame: DataFrame with blank strings replaced by None.

    """
    transactions_df.replace(" ", None, inplace=True)
    return transactions_df


@loggable
def extract_tags(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Extract tags from the 'tags' column, keeping only hashtags.

    Args:
        transactions_df (pd.DataFrame): DataFrame with a 'tags' column containing text data.

    Returns:
        pd.DataFrame: DataFrame with hashtags extracted and formatted as lists.

    """

    def extract_hashtag(tag: str) -> list[str]:
        if isinstance(tag, str):
            match = re.search(r"#(\w+)", tag)
            if match:
                return ["#" + match.group(1)]
        return []

    transactions_df["tags"] = transactions_df["tags"].apply(extract_hashtag)
    return transactions_df


@loggable
def filter_declined_transactions(new_transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Filter out transactions marked as declined.

    Args:
        new_transactions_df (pd.DataFrame): DataFrame 'decline' column for declined transactions.

    Returns:
        pd.DataFrame: DataFrame excluding declined transactions.

    """
    return new_transactions_df[new_transactions_df["decline"] == 0]


@loggable
def dataframe_to_dict(transactions_df: pd.DataFrame) -> Any:
    """Convert the DataFrame to a dictionary in JSON-like format.

    Args:
        transactions_df (pd.DataFrame): DataFrame to convert.

    Returns:
        list[dict[str, Any]]: List of dictionary representations of the DataFrame in JSON-like format.

    """
    return loads(transactions_df.to_json(orient="records"))


@loggable
def get_lunch_money_assets() -> dict:
    """Get Lunch Money assets.

    Returns:
        dict: Dictionary of Lunch Money asset detals.

    """
    lunch_money_assets_path = Path(os.getenv("LUNCH_MONEY_ASSETS_PATH", ""))
    with lunch_money_assets_path.open() as f:
        asset_ids_map = loads(f.read())
    return {asset_id["display_name"]: int(asset_id["id"]) for asset_id in asset_ids_map["assets"]}


@loggable
def get_lunch_money_categories() -> dict[str, int]:
    """Get Lunch Money categories.

    Returns:
        dict: Dictionary of Lunch Money cateogory detals.

    """
    lunch_money_categories_path = Path(os.getenv("LUNCH_MONEY_CATEGORIES_PATH", ""))
    with lunch_money_categories_path.open() as f:
        categories_map = loads(f.read())
    return {category["name"]: category["id"] for category in categories_map["categories"]}
