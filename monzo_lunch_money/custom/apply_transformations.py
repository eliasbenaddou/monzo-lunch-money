import os
from json import loads
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from ntfy_wrapper import Notifier

logger = CustomLogger.get_logger()

all_categories = [
    "Education",
    "Clothes",
    "Withdrawals",
    "Gym",
    "Hotels",
    "Healthcare",
    "ISA",
    "Bills",
    "Entertainment",
    "Transfers",
    "Eating Out",
    "Savings",
    "Travel",
    "Subscriptions",
    "Groceries",
    "Income",
    "Gifts",
    "Home",
    "Crypto",
    "Pets",
    "Transport",
    "Fees",
    "Shopping",
]


@loggable
def get_pot_acc_names() -> dict[str, str]:
    """Get the Pot account names.

    Returns:
        pd.DataFrame: Datamframe of transformed main transactions

    """
    path = Path("monzo_lunch_money/shared_info/pot_account_ids.json")
    with path.open() as pot_account_ids:
        pot_id_names_dct = loads(pot_account_ids.read())
        return {pot["id"]: pot["name"] for pot in pot_id_names_dct["pots"]}


@loggable
def get_main_transsactions_df(main_transactions_dct: dict) -> pd.DataFrame:
    """Get main transactions.

    Args:
        main_transactions_dct (dict): Dictionary of main transactions

    Returns:
        pd.DataFrame: Datamframe of transformed main transactions

    """
    main_transactions_dfs_lst = [pd.DataFrame(dct) for dct in main_transactions_dct.values()]
    return pd.concat(main_transactions_dfs_lst, ignore_index=True)


@loggable
def get_pot_transsactions_df(pot_transactions_dct: dict) -> pd.DataFrame:
    """Get pot transactions.

    Args:
        pot_transactions_dct (dict): Dictionary of pot transactions

    Returns:
        pd.DataFrame: Datamframe of transformed main transactions

    """
    pot_transactions_dfs_lst = [pd.DataFrame(dct) for dct in pot_transactions_dct.values()]
    return pd.concat(pot_transactions_dfs_lst, ignore_index=True)


@loggable
def apply_transformations(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to transactions.

    Args:
        transactions_df: pd.DataFrame: Dataframe of transactions to apply transformations to.

    Returns:
        pd.DataFrame: Single dataframe resulting from concatenating all input dataframes,

    """
    logger.debug("Normalising dataframe to flatten nested rows")
    transactions_meta_df = merge_normalise_column(transactions_df, "meta")
    transactions_meta_merchant_data_df = merge_normalise_column(transactions_meta_df, "merchant")

    logger.debug("Renaming columns after merging")
    transactions_renamed_cols = rename_merged_cols(transactions_meta_merchant_data_df)

    logger.debug("Applying formatting and filtering to columns")
    transactions_df_formatted_amounts = format_amounts(transactions_renamed_cols)

    logger.debug("Convert amount and local_amount to appropriate currencies")
    transactions_df_set_amount_currency = set_amount_currency(transactions_df_formatted_amounts)

    logger.debug("Keeping original currency and amount where applicable")
    transactions_df_set_amount_value = set_amount_value(transactions_df_set_amount_currency)

    logger.debug("Creating decline column based on decline_reason presence")
    transactions_df_decline_col = add_decline_column(transactions_df_set_amount_value)

    logger.debug("Droping unnecessary columns and resetting index")
    transactions_df_cleaned = drop_cols_and_reset_index(transactions_df_decline_col)

    logger.debug("Ensure all columns have a populated value or None")
    transactions_df_populated = ensure_all_cols_populated(transactions_df_cleaned)

    logger.debug("Using merchant_description in place of description where available")
    transactions_merchant_desc = set_descriptions(transactions_df_populated)

    logger.debug("Renaming suggested_tags to tags and dropping merchant_description")
    transactions_tags = set_tags(transactions_merchant_desc)

    logger.debug("Sorting transactions by date")
    transactions_sorted = sort_by_date(transactions_tags)

    logger.debug("Mapping pot account IDs to human-readable names")
    pot_id_names_dct = get_pot_acc_names()
    transactions_pot_acc_ids = map_pot_acc_ids(transactions_sorted, pot_id_names_dct)

    logger.debug("Replacing descriptions starting with PB with values from the notes column")
    transactions_pb_transactions_desc = replace_pb_transactions_desc(transactions_pot_acc_ids)

    logger.debug("Converting and formatting date columns")
    transactions_dates_formatted = format_date_columns(transactions_pb_transactions_desc)

    logger.debug("Sorting the transactions by timestamp in descending order")
    transactions_timestamp_sorted = sort_by_timestamp_descending(transactions_dates_formatted)

    logger.debug("Convert datetime columns to strings")
    transactions_datetime_cols_str = assign_date_cols_to_str(transactions_timestamp_sorted)

    logger.debug("Format the categories to use names instead of IDs")
    categories = get_categories()
    transactions_df_categories_format = format_categories(
        transactions_datetime_cols_str, categories
    )

    logger.debug("Dropping transactions if category is unknown")
    transactions_df_no_unknown_category = drop_transactions_unknown_category(
        transactions_df_categories_format
    )

    logger.debug("Selecting columns")
    transactions_df = select_cols(transactions_df_no_unknown_category)

    logger.debug(f"Transformed {len(transactions_df)} transactions from all sources")

    return transactions_df


@loggable
def merge_normalise_column(df: pd.DataFrame, column: str) -> Any:
    """Merge the normalised column back into the dataframe.

    Expand nested JSON structures in a specified DataFrame column using json_normalize and
    merges the result back into the original dataframe using the index.

    Args:
        df (pd.DataFrame): The input DataFrame containing the column to normalize.
        column (str): The name of the column containing JSON-like data to normalize.

    Returns:
        pd.DataFrame: A new DataFrame with the specified column normalized and flattened.

    """
    normalised_df = pd.json_normalize(df[column])  # type: ignore[arg-type]
    return df.drop(columns=[column]).merge(normalised_df, left_index=True, right_index=True)


@loggable
def format_categories(
    transactions_df: pd.DataFrame, category_replacements_dct: dict
) -> pd.DataFrame:
    """Format and replace category codes with names.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions
        category_replacements_dct (dict): Dictionary of formatted names for custom Monzo categories

    Returns:
        pd.Dataframe: Transactions dataframe with formatted category column.

    """
    transactions_df["category"] = transactions_df["category"].replace(category_replacements_dct)
    transactions_df["category"] = transactions_df["category"].str.replace("_", " ").str.title()

    return transactions_df


@loggable
def format_amounts(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Format the amount column to be in pounds and set to positive numbers.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["amount"] = -transactions_df["amount"] / 100
    transactions_df["local_amount"] = -transactions_df["local_amount"] / 100

    return transactions_df


@loggable
def set_amount_currency(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Set the currency column based on the local currency.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["currency"] = np.where(
        transactions_df["local_currency"] != "GBP",
        transactions_df["local_currency"].str.lower(),
        transactions_df["currency"].str.lower(),
    )

    return transactions_df


@loggable
def set_amount_value(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Set the amount column based on the currency.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["amount"] = np.where(
        transactions_df["local_currency"] == "GBP",
        transactions_df["amount"],
        transactions_df["local_amount"],
    )

    return transactions_df


@loggable
def add_decline_column(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Adds the decline column based on other columns.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["decline"] = np.where(transactions_df["decline_reason"].fillna("") == "", 0, 1)

    return transactions_df


@loggable
def drop_cols_and_reset_index(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Drops columns no longer required and resets the index.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df.drop(columns=["local_currency"], inplace=True)
    transactions_df.reset_index(drop=True, inplace=True)

    return transactions_df


@loggable
def ensure_all_cols_populated(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Ensures all columns are populated with a value or None.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    columns = [
        "id",
        "date",
        "description",
        "amount",
        "category",
        "merchant_description",
        "notes",
        "suggested_tags",
        "decline",
        "decline_reason",
        "currency",
        "source",
    ]
    for col in columns:
        if col not in transactions_df.columns:
            transactions_df[col] = None

    return transactions_df


@loggable
def set_descriptions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Sets the description to use the merchant description column where available.

    Drops the merchant column after using it.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["description"] = np.where(
        transactions_df["merchant_description"].notnull(),
        transactions_df["merchant_description"],
        transactions_df["description"],
    )
    transactions_df.drop(columns=["merchant_description"], inplace=True)

    return transactions_df


@loggable
def set_tags(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Sets the tags to use the suggested_tags column.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df.rename(columns={"suggested_tags": "tags"}, inplace=True)

    return transactions_df


@loggable
def sort_by_date(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Sorts the dataframe by the datdataframe by the date.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df.sort_values("date", inplace=True)
    return transactions_df


@loggable
def map_pot_acc_ids(transactions_df: pd.DataFrame, pot_id_names_dct: dict) -> pd.DataFrame:
    """Map the pot account IDS to humand readable names.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions.
        pot_id_names_dct (dict): Dictionary of pot id and pot names.

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["description"] = np.where(
        ~transactions_df["description"].map(pot_id_names_dct).isna(),
        transactions_df["description"].map(pot_id_names_dct),
        transactions_df["description"],
    )

    return transactions_df


@loggable
def replace_pb_transactions_desc(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Replaces the description for transactions beginning with 'PB' with the notes
    columns.

    Useful for getting Monzo transactions notes for American Express transfers.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["description"] = np.where(
        transactions_df["description"].str.startswith("PB"),
        transactions_df["notes"],
        transactions_df["description"],
    )
    return transactions_df


@loggable
def format_date_columns(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Format the date columns.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    transactions_df["timestamp"] = pd.to_datetime(transactions_df["date"])
    transactions_df["date"] = pd.to_datetime(transactions_df["date"]).dt.date

    return transactions_df


@loggable
def sort_by_timestamp_descending(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Sorts the dataframe by the timestamp descending.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    return transactions_df.sort_values("timestamp", ascending=False)


@loggable
def assign_date_cols_to_str(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Convert all datetime columns in the transactions DataFrame to string type.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions.

    Returns:
        pd.DataFrame: Dataframe of transactions with datetime columns converted to strings.

    """
    # Identify datetime columns
    datetime_cols = transactions_df.select_dtypes(include=["datetime"]).columns

    # Convert datetime columns to string type and reassign
    return transactions_df.assign(**{
        col: transactions_df[col].astype(str) for col in datetime_cols
    })


@loggable
def replace_empty_str_with_none(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Replace emptry strings with None.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    return transactions_df.replace("", None)


@loggable
def select_cols(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Limit the dataframe to the select columns.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions

    Returns:
        pd.Dataframe: Dataframe of transactions with formatted amount columns

    """
    return transactions_df[
        [
            "id",
            "date",
            "description",
            "timestamp",
            "amount",
            "category",
            "notes",
            "decline_reason",
            "tags",
            "decline",
            "currency",
            "source",
        ]
    ]


@loggable
def rename_merged_cols(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Rename the merged columns.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions.

    Returns:
        pd.DataFrame: Dataframe of transactions with columns renamed.

    """
    columns_dict = {
        "id_x": "id",
        "id_y": "merchant_id",
        "name": "merchant_description",
        "category_x": "category",
        "category_y": "merchant_category",
    }
    transactions_df.rename(columns=columns_dict, inplace=True)

    return transactions_df


@loggable
def drop_transactions_unknown_category(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Drop transactions where the category is not part of the standard or custom
    categories.

    Args:
        transactions_df (pd.DataFrame): Dataframe of transactions.

    Returns:
        pd.DataFrame: Dataframe of transactions with columns renamed.

    """
    if not transactions_df["category"].isin(all_categories).all():
        unknown_category_transactions = [
            (
                transaction["id"],
                transaction["date"],
                transaction["category"],
                transaction["description"],
                transaction["source"],
                transaction["decline"],
            )
            for _, transaction in transactions_df[
                ~transactions_df["category"].isin(all_categories)
            ].iterrows()
        ]

        ntfy = Notifier()
        ntfy.notify(
            f"{len(unknown_category_transactions)} or more undefined category in transactions: {unknown_category_transactions}"
        )
        logger.warning("There are rows with unknown categories - omitting these transactions")
        return transactions_df[transactions_df["category"].isin(all_categories)]

    return transactions_df


@loggable
def get_categories() -> Any:
    """Get the categories from the config file.

    Returns:
        dict: Dictionary of categoy ID's and names

    """
    path = Path(os.getenv("MONZO_CATEGORIES_PATH", ""))
    with path.open("r") as f:
        return loads(f.read())
