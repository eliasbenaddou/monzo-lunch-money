import pandas as pd
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable

logger = CustomLogger.get_logger()


@loggable
def identify_changed_transactions(
    changed_transactions_ids: list[str], transactions_df: pd.DataFrame
) -> pd.DataFrame:
    """Identify modified transactions by comparing fetched transactions to database.

    Args:
        changed_transactions_ids (list[str]): List of transactions IDs of modified transactions in Monzo.
        transactions_df (pd.DataFrame): Dataframe of fetched transactions.

    Returns:
        pd.DataFrame: Dataframe of fetched transactions that are identified as modified in Monzo.

    """
    changed_transactions_df = transactions_df[transactions_df["id"].isin(changed_transactions_ids)]
    logger.debug(f"Identified {len(changed_transactions_df)} changed transactions")

    return changed_transactions_df
