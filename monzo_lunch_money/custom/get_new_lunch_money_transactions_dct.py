from typing import Any

import pandas as pd
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable

from monzo_lunch_money.custom.apply_lunch_money_transformations import (
    assign_payee_column,
    dataframe_to_dict,
    extract_tags,
    filter_declined_transactions,
    format_date_column,
    get_lunch_money_assets,
    get_lunch_money_categories,
    map_asset_id,
    map_category_id,
    replace_blank_with_none,
)

logger = CustomLogger.get_logger()


@loggable
def get_new_lunch_money_transactions_dct(new_transactions_df: pd.DataFrame) -> Any:
    """Prepare dictionary of new transactions to upload to Lunch Money.

    Args:
        new_transactions_df (pd.DataFrame): DataFrame of new transactions to upload to Lunch Money.

    Returns:
        dict: Dictionary of new transactions for Lunch Money.

    """
    categories_dict = get_lunch_money_categories()
    assets_ids_dict = get_lunch_money_assets()

    logger.debug("Map category IDs", new_transactions_df.head())
    new_transactions_df = map_category_id(new_transactions_df, categories_dict)

    logger.debug("Map asset IDs", new_transactions_df.head())
    new_transactions_df = map_asset_id(new_transactions_df, assets_ids_dict)

    logger.debug("Set external IDs", new_transactions_df.head())
    new_transactions_df["external_id"] = new_transactions_df["id"]

    logger.debug("Format date column", new_transactions_df.head())
    new_transactions_df = format_date_column(new_transactions_df)

    logger.debug("Assign payee column", new_transactions_df.head())
    new_transactions_df = assign_payee_column(new_transactions_df)

    logger.debug("Replace blank strings with None", new_transactions_df.head())
    new_transactions_df = replace_blank_with_none(new_transactions_df)

    logger.debug("Extract tagss", new_transactions_df.head())
    new_transactions_df = extract_tags(new_transactions_df)

    logger.debug("Filter declined transactions", new_transactions_df.head())
    new_transactions_df = filter_declined_transactions(new_transactions_df)

    logger.debug("Select final columns", new_transactions_df.head())
    new_transactions_df = select_final_new_columns(new_transactions_df)

    logger.debug("Converted DataFrame to dictionary", new_transactions_df)
    return dataframe_to_dict(new_transactions_df)


@loggable
def select_final_new_columns(new_transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Select and order the final columns for the output.

    Args:
        new_transactions_df (pd.DataFrame): DataFrame with various columns.

    Returns:
        pd.DataFrame: DataFrame with only the selected columns in the required order.

    """
    return new_transactions_df[
        [
            "date",
            "payee",
            "amount",
            "notes",
            "category_id",
            "tags",
            "external_id",
            "asset_id",
            "currency",
        ]
    ]
