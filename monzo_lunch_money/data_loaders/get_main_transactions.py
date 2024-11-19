import asyncio

import pandas as pd
from monzo.authentication import Authentication
from monzo_api_wrapper.get_transactions import get_transactions_df
from monzo_api_wrapper.utils.custom_logger import loggable


@loggable
async def get_main_transactions(
    monzo_auth_obj: Authentication, days_lookback: int, main_accounts: dict
) -> dict[str, pd.DataFrame]:
    """Get the main accounts transactions.

    Args:
        monzo_auth_obj (Authentication): Authentication object
        days_lookback (int): Number of days to look back in fetching results
        main_accounts (dict): Dictionary of main account details

    Returns:
        dict of main accounts transactions

    """

    async def fetch_transactions(acc_name: str, acc_id: str) -> tuple[str, pd.DataFrame]:
        df = await asyncio.to_thread(
            get_transactions_df,
            days_lookback=days_lookback,
            monzo_auth=monzo_auth_obj,
            account_id=acc_id,
            account_name=acc_name,
        )
        return acc_name, df

    tasks = [fetch_transactions(acc_name, acc_id) for acc_name, acc_id in main_accounts.items()]

    results = await asyncio.gather(*tasks)
    return dict(results)
