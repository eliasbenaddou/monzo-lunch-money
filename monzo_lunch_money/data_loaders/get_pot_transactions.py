import asyncio

import pandas as pd
from monzo.authentication import Authentication
from monzo_api_wrapper.get_transactions import get_transactions_df
from monzo_api_wrapper.utils.custom_logger import loggable


@loggable
async def get_pot_transactions(
    monzo_auth_obj: Authentication, days_lookback: int, pot_accounts: dict
) -> dict:
    """Get the Pot transactions.

    Args:
        monzo_auth_obj (Authentication): Authentication object
        days_lookback (int): Number of days to look back in fetching results
        pot_accounts (dict): Dictionary of pot account details

    Returns:
        dict of Pot transactions

    """

    async def fetch_transactions(acc_id: str, acc_name: str) -> tuple[str, pd.DataFrame]:
        df = await asyncio.to_thread(
            get_transactions_df,
            days_lookback=days_lookback,
            monzo_auth=monzo_auth_obj,
            account_id=acc_id,
            account_name=f"{acc_name} Pot",
        )
        return acc_name, df

    tasks = [fetch_transactions(acc_id, acc_name) for acc_id, acc_name in pot_accounts.values()]

    results = await asyncio.gather(*tasks)
    return dict(results)
