#!/root/betting/betenv/bin/python

"""
close all open Betfair streaming subscriptions.
"""

import os
from dotenv import load_dotenv
import betfairlightweight

def main():
    # load environment variables from .env in current directory
    load_dotenv()
    username = os.getenv("BETFAIR_USERNAME")
    password = os.getenv("BETFAIR_PASSWORD")
    app_key  = os.getenv("BETFAIR_APP_KEY")
    certs    =     "/root/betting/certs"

    trading = betfairlightweight.APIClient(
        username,
        password,
        app_key=app_key,
        certs=certs
    )

    trading.login()

    # cancel all unmatched orders (optional)
    try:
        trading.betting.cancel_orders()
        print("All unmatched orders cancelled.")
    except Exception as e:
        print(f"No orders to cancel or error: {e}")

    # close every active streaming subscription
    try:
        trading.streaming.close()
        print("All streaming connections closed.")
    except Exception as e:
        print(f"Streaming close error: {e}")

    trading.logout()
    print("Subscriptions cleared and logged out.")

if __name__ == "__main__":
    main()
