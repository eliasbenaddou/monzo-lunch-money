import os
from typing import Any

import pandas as pd
from monzo_api_wrapper.upload_transactions import get_new_transactions
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from monzo_api_wrapper.utils.db import Db

logger = CustomLogger.get_logger()


@loggable
def identify_new_transactions(db: Db, transactions_df: pd.DataFrame) -> Any:
    """Identify new transactions that are not yet in the database.

    Args:
        db (Db): Database connection object.
        transactions_df (pd.DataFrame): Dataframe of transactions.

    Returns:
        pd.DataFrame: Dataframe containing only new transactions.

    """
    new_transactions_df = get_new_transactions(
        db=db, table=os.getenv("DB_TABLE"), fetched_transactions=transactions_df
    )
    logger.debug(f"Identified {len(new_transactions_df)} new transactions")

    return new_transactions_df
