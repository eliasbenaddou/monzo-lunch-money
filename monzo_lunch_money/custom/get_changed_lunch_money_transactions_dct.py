from typing import Any

import pandas as pd
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable

from monzo_lunch_money.custom.apply_lunch_money_transformations import (
    assign_payee_column,
    dataframe_to_dict,
    extract_tags,
    format_date_column,
    get_lunch_money_assets,
    get_lunch_money_categories,
    map_asset_id,
    map_category_id,
    replace_blank_with_none,
)

logger = CustomLogger.get_logger()


@loggable
def get_changed_lunch_money_transactions_dct(changed_transactions_df: pd.DataFrame) -> Any:
    """Prepare dictionary of changed transactions to update in Lunch Money.

    Args:
        changed_transactions_df (pd.DataFrame): DataFrame of new transactions to upload to Lunch Money.

    Returns:
        dict: Dictionary of changed transactions for Lunch Money.

    """
    categories_dict = get_lunch_money_categories()
    assets_ids_dict = get_lunch_money_assets()

    logger.debug("Map category IDs", changed_transactions_df.head())
    changed_transactions_df = map_category_id(changed_transactions_df, categories_dict)

    logger.debug("Map asset IDs", changed_transactions_df.head())
    changed_transactions_df = map_asset_id(changed_transactions_df, assets_ids_dict)

    logger.debug("Set external IDs", changed_transactions_df.head())
    changed_transactions_df["external_id"] = changed_transactions_df["id"]

    logger.debug("Format date column", changed_transactions_df.head())
    changed_transactions_df = format_date_column(changed_transactions_df)

    logger.debug("Assig payee column", changed_transactions_df.head())
    changed_transactions_df = assign_payee_column(changed_transactions_df)

    logger.debug("Replace blank strings with None", changed_transactions_df.head())
    changed_transactions_df = replace_blank_with_none(changed_transactions_df)

    logger.debug("Extract tagss", changed_transactions_df.head())
    changed_transactions_df = extract_tags(changed_transactions_df)

    logger.debug("Select final columns", changed_transactions_df.head())
    changed_transactions_df = select_final_update_columns(changed_transactions_df)

    logger.debug("Convert DataFrame to dictionary", changed_transactions_df)
    return dataframe_to_dict(changed_transactions_df)


@loggable
def select_final_update_columns(changed_transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Select and order the final columns for the output.

    Args:
        changed_transactions_df (pd.DataFrame): DataFrame with various columns.

    Returns:
        pd.DataFrame: DataFrame with only the selected columns in the required order.

    """
    changed_transactions_df["transaction_id"] = changed_transactions_df["id"]
    return changed_transactions_df[
        [
            "lunch_money_id",
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
