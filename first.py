import betfairlightweight
print('hell0')

trading = betfairlightweight.APIClient(
    username="hcoussens1",
    app_key="zLsKKtBX7Z15FuSf",
    certs=("./certs/client-2048.crt", "./certs/client-2048.key")
)
trading.session_token = "your_saved_session_token"

