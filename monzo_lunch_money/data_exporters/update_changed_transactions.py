import os
from typing import Any

import pandas as pd
from monzo_api_wrapper.utils import sql_templates
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from monzo_api_wrapper.utils.db import Db

logger = CustomLogger.get_logger()


@loggable
def update_changed_transactions(db: Db, changed_transactions_df: pd.DataFrame) -> None:
    """Upload changed transactions to the database.

    Args:
        db (Db): Database connection object.
        changed_transactions_df: Dataframe of changed transactions to update.

    Raises:
        Exception: When an error occurs in the updating of transactinons.

    """
    changed_transactions_df = add_lunch_money_ids(db, changed_transactions_df)
    try:
        transactions_to_delete_ids_str = (
            str(changed_transactions_df["id"].to_list()).strip("[").strip("]")
        )
        logger.info(transactions_to_delete_ids_str)
        db.delete(table=os.getenv("DB_TABLE", ""), data=transactions_to_delete_ids_str)
        db.insert(table=os.getenv("DB_TABLE", ""), df=changed_transactions_df)
        logger.info(f"Updated {len(changed_transactions_df)} changed transactions")
    except Exception:
        raise Exception("An error occurred while updating changed transactions") from None


@loggable
def get_lunch_money_id(db: Db, monzo_transaction_id: str) -> Any:
    """Get the Lunch Money id from the database.

    Args:
        db (Db): Database connection object.
        monzo_transaction_id (str): Monzo transaction ID.

    Returns:
        str: Series containing Lunch Money IDs

    """
    lunch_money_id = db.query(
        sql=sql_templates.get_id.format(
            table=os.getenv("DB_TABLE"), external_id=monzo_transaction_id
        )
    )
    if lunch_money_id is not None:
        return lunch_money_id["lunch_money_id"]
    return None


@loggable
def add_lunch_money_ids(db: Db, changed_transactions: pd.DataFrame) -> pd.DataFrame:
    """Apply function to get Lunch Money IDs.

    Args:
        db (Db): Database connection object
        changed_transactions (pd.DataFram): DataFrame of changed transactions

    Returns:
        pd.DataFrame: Dataframe of changed transactions with lunch_money_id field populated.

    """
    changed_transactions["lunch_money_id"] = changed_transactions["id"].apply(
        lambda x: get_lunch_money_id(db, x)
    )
    return changed_transactions
