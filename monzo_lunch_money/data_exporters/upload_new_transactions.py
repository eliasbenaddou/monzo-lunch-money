import os

import pandas as pd
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from monzo_api_wrapper.utils.db import Db

logger = CustomLogger.get_logger()


@loggable
def upload_new_transactions(db: Db, new_transactions_df: pd.DataFrame) -> None:
    """Uploads new transactions to the database.

    Args:
        db (Db): Database connection object.
        new_transactions_df: Dataframe of new transactions to upload.

    Raises:
        Exception: When an error occurs in the inserting of transactinons.

    """
    try:
        db.insert(table=os.getenv("DB_TABLE", ""), df=new_transactions_df)
        logger.debug(f"Uploaded {len(new_transactions_df)} new transactions")
    except Exception as e:
        raise Exception("An error occurred while uploading new transactions: " + str(e)) from e
