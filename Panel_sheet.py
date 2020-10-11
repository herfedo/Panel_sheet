import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pyRofex
import time
import config

scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scopes)
client = gspread.authorize(creds)

# Nombre de la planilla y de la hoja
wks = client.open("Bonos").worksheet("Panel")

instrumentos = [
    "MERV - XMEV - AL29 - 48hs",
    "MERV - XMEV - AL30 - 48hs",
    "MERV - XMEV - AL35 - 48hs",
    "MERV - XMEV - AE38 - 48hs",
    "MERV - XMEV - AL41 - 48hs",
    "MERV - XMEV - GD29 - 48hs",
    "MERV - XMEV - GD30 - 48hs",
    "MERV - XMEV - GD35 - 48hs",
    "MERV - XMEV - GD38 - 48hs",
    "MERV - XMEV - GD41 - 48hs",
    "MERV - XMEV - GD46 - 48hs",
    "MERV - XMEV - AL29D - 48hs",
    "MERV - XMEV - AL30D - 48hs",
    "MERV - XMEV - AL35D - 48hs",
    "MERV - XMEV - AE38D - 48hs",
    "MERV - XMEV - AL41D - 48hs",
    "MERV - XMEV - GD29D - 48hs",
    "MERV - XMEV - GD30D - 48hs",
    "MERV - XMEV - GD35D - 48hs",
    "MERV - XMEV - GD38D - 48hs",
    "MERV - XMEV - GD41D - 48hs",
    "MERV - XMEV - GD46D - 48hs",
    "MERV - XMEV - AL29 - CI",
    "MERV - XMEV - AL30 - CI",
    "MERV - XMEV - AL35 - CI",
    "MERV - XMEV - AE38 - CI",
    "MERV - XMEV - AL41 - CI",
    "MERV - XMEV - GD29 - CI",
    "MERV - XMEV - GD30 - CI",
    "MERV - XMEV - GD35 - CI",
    "MERV - XMEV - GD38 - CI",
    "MERV - XMEV - GD41 - CI",
    "MERV - XMEV - GD46 - CI",
    "MERV - XMEV - AL29D - CI",
    "MERV - XMEV - AL30D - CI",
    "MERV - XMEV - AL35D - CI",
    "MERV - XMEV - AE38D - CI",
    "MERV - XMEV - AL41D - CI",
    "MERV - XMEV - GD29D - CI",
    "MERV - XMEV - GD30D - CI",
    "MERV - XMEV - GD35D - CI",
    "MERV - XMEV - GD38D - CI",
    "MERV - XMEV - GD41D - CI",
    "MERV - XMEV - GD46D - CI",
    "MERV - XMEV - TO23 - 48hs",
    "MERV - XMEV - TO26 - 48hs",
    "MERV - XMEV - PARP - 48hs",
    "MERV - XMEV - TX26 - 48hs",
    "MERV - XMEV - TX28 - 48hs",
    "MERV - XMEV - TC23 - 48hs",
    "MERV - XMEV - AL29C - 48hs",
    "MERV - XMEV - AL30C - 48hs",
    "MERV - XMEV - AL35C - 48hs",
    "MERV - XMEV - AE38C - 48hs",
    "MERV - XMEV - AL41C - 48hs",
    "MERV - XMEV - GD29C - 48hs",
    "MERV - XMEV - GD30C - 48hs",
    "MERV - XMEV - GD35C - 48hs",
    "MERV - XMEV - GD38C - 48hs",
    "MERV - XMEV - GD41C - 48hs",
    "MERV - XMEV - GD46C - 48hs"
]


prices = pd.DataFrame(columns=["Last", "Bid_size", "Bid", "Ask_size", "Ask"], index=instrumentos).fillna(0)
prices.index.name = "Instrumento"


pyRofex.initialize(user=config.usuario,
                   password=config.password,
                   account=config.comitente,
                   environment=pyRofex.Environment.LIVE)

def market_data_handler(message):
    global prices

    if message["marketData"]["LA"] == None:
        prices.loc[message["instrumentId"]["symbol"], 'Last'] = 0
    else:
        prices.loc[message["instrumentId"]["symbol"], 'Last'] = message["marketData"]["LA"]["price"]

    if message["marketData"]["OF"] == None:
        prices.loc[message["instrumentId"]["symbol"], 'Ask'] = 0
    else:
        prices.loc[message["instrumentId"]["symbol"], 'Ask'] = message["marketData"]["OF"][0]["price"]
        prices.loc[message["instrumentId"]["symbol"], 'Ask_size'] = message["marketData"]["OF"][0]["size"]

    if message["marketData"]["BI"] == None:
        prices.loc[message["instrumentId"]["symbol"], 'Bid'] = 0
    else:
        prices.loc[message["instrumentId"]["symbol"], 'Bid'] = message["marketData"]["BI"][0]["price"]
        prices.loc[message["instrumentId"]["symbol"], 'Bid_size'] = message["marketData"]["BI"][0]["size"]

pyRofex.init_websocket_connection(market_data_handler=market_data_handler)

entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS, pyRofex.MarketDataEntry.LAST]

pyRofex.market_data_subscription(
    tickers=instrumentos,
    entries=entries
)

while True:
    try:
        wks.update('A1', [prices.reset_index().columns.tolist()] + prices.reset_index().values.tolist())
    except:
        time.sleep(1)

    time.sleep(1)