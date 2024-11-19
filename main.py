import argparse
import asyncio
import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from monzo.authentication import Authentication
from monzo_api_wrapper.upload_transactions import get_changed_transaction_ids
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from monzo_api_wrapper.utils.db import Db

from monzo_lunch_money.custom.apply_transformations import (
    apply_transformations,
    get_main_transsactions_df,
    get_pot_transsactions_df,
)
from monzo_lunch_money.custom.get_changed_lunch_money_transactions_dct import (
    get_changed_lunch_money_transactions_dct,
)
from monzo_lunch_money.custom.get_monzo_auth import get_monzo_auth
from monzo_lunch_money.custom.get_new_lunch_money_transactions_dct import (
    get_new_lunch_money_transactions_dct,
)
from monzo_lunch_money.custom.identify_changed_transactions import identify_changed_transactions
from monzo_lunch_money.custom.identify_new_transactions import identify_new_transactions
from monzo_lunch_money.data_exporters.update_changed_lunch_money_transactions import (
    update_changed_lunch_money_transactions,
)
from monzo_lunch_money.data_exporters.update_changed_transactions import update_changed_transactions
from monzo_lunch_money.data_exporters.upload_new_lunch_money_transactions import (
    upload_new_lunch_money_transactions,
)
from monzo_lunch_money.data_exporters.upload_new_transactions import upload_new_transactions
from monzo_lunch_money.data_loaders.get_main_transactions import get_main_transactions
from monzo_lunch_money.data_loaders.get_pot_transactions import get_pot_transactions

pd.set_option("mode.chained_assignment", None)
logger = CustomLogger.get_logger()


@loggable
def get_transactions(
    monzo_auth_obj: Authentication,
    days_lookback: int,
    main_accounts: dict,
    pot_accounts: dict | None,
    include_pots: bool = True,
) -> pd.DataFrame:
    """Fetch transactions from Monzo.

    Args:
        monzo_auth_obj (Authentication): Authentication object
        days_lookback (int): Number of days to lookback in fetching results
        main_accounts (dict): Dictionary of main account details
        pot_accounts (dict): Dictionary of pot account details
        include_pots (bool): Flag to include Pots in transactions processing

    Returns:
        pd.DataFrame: Dataframe of transactions fetched.

    """
    transactions_dct = asyncio.run(
        get_main_transactions(
            monzo_auth_obj=monzo_auth_obj, days_lookback=days_lookback, main_accounts=main_accounts
        )
    )

    transactions_df = get_main_transsactions_df(transactions_dct)

    if include_pots:
        pot_transactions_dct = asyncio.run(
            get_pot_transactions(
                monzo_auth_obj=monzo_auth_obj,
                days_lookback=days_lookback,
                pot_accounts=pot_accounts,
            )
        )
        pot_transactions_df = get_pot_transsactions_df(pot_transactions_dct)

        logger.debug(
            "Combining main account and pot transactions into one dataframe for processing"
        )
        return pd.concat([transactions_df, pot_transactions_df], ignore_index=True)
    return transactions_df


@loggable
def get_source_accounts() -> tuple[dict, dict]:
    """Get Monzo source account details.

    Returns:
        tuple: Main and pot accounts details.

    """
    path = Path(os.getenv("MONZO_SOURCE_ACCOUNTS_PATH"))
    with path.open("r") as f:
        source_accounts = json.loads(f.read())

    main_accounts = source_accounts["MAIN_ACCOUNTS"]
    pot_accounts = source_accounts["POT_ACCOUNTS"]

    return main_accounts, pot_accounts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update Lunch Money personal and joint budgets with Monzo transactions"
    )
    parser.add_argument(
        "days_lookback",
        type=int,
        help="Must be less than 90 days unless re-authroised in Monzo app",
        default=30,
    )

    parser.add_argument(
        "env",
        choices=["personal", "joint"],
        help="Specify the account (personal or joint)",
    )

    parser.add_argument(
        "pots",
        choices=["True", "False"],
        help="Specify whether to get pot transactions too",
        default="True",
    )

    args = parser.parse_args()

    account_env = f".env.{args.env}"
    load_dotenv(account_env)

    auth = get_monzo_auth()
    main_accounts, pot_accounts = get_source_accounts()

    transactions_df = get_transactions(
        monzo_auth_obj=auth,
        days_lookback=args.days_lookback,
        main_accounts=main_accounts,
        pot_accounts=None if args.pots.lower() == "false" else pot_accounts,
        include_pots=args.pots.lower() != "false",
    )
    if transactions_df.empty:
        logger.info("No transactions fetched.")
    else:
        db = Db()
        transactions_df = apply_transformations(transactions_df)
        new_transactions_df = identify_new_transactions(db=db, transactions_df=transactions_df)
        if len(new_transactions_df) > 0:
            upload_new_transactions(db=db, new_transactions_df=new_transactions_df)
            new_transactions_dct = get_new_lunch_money_transactions_dct(new_transactions_df)
            upload_new_lunch_money_transactions(db=db, transactions_dct_lst=new_transactions_dct)
            logger.info(f"{len(new_transactions_df)} new transactions uploaded to Lunch Money!")
        else:
            logger.info("No new transactions to upload")

        changed_transactions_ids = get_changed_transaction_ids(
            db=db, table=os.getenv("DB_TABLE"), fetched_transactions=transactions_df
        )
        if len(changed_transactions_ids) == 0:
            logger.info("No modified transactions to update")
        else:
            changed_transactions_df = identify_changed_transactions(
                changed_transactions_ids=changed_transactions_ids, transactions_df=transactions_df
            )
            update_changed_transactions(db, changed_transactions_df)
            changed_transactions_dct = get_changed_lunch_money_transactions_dct(
                changed_transactions_df
            )
            update_changed_lunch_money_transactions(changed_transactions_dct)
            logger.info(
                f"{len(changed_transactions_df)} modified transactions updated in Lunch Money!"
            )
