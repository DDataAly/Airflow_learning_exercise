# BTC Best Price Tracker – Find the Cheapest BTC Price Across Exchanges

## 🔰 Overview

A simple CLI tool that finds the **best available price** for purchasing a given amount of BTC by querying multiple cryptocurrency exchanges.  
It implements a **Dollar Cost Averaging** approach, splitting the total purchase into smaller transactions spaced out over time.

## 🔧 Tech Stack
- **Python 3.12** – primary programming language
- **Pandas** – for tabular data manipulation
- **Requests** – for interacting with REST APIs


## 📊 How It Works

- Pulls **order book data** from 3 major exchanges: **Binance**, **Coinbase**, and **Kraken**
- Determines the **cheapest exchange** for each transaction
- Saves each order book snapshot and best deal data locally
- Calculates the **total cost** for the entire order after execution

> 🧠 I'm currently working on a follow-up project using **WebSocket streaming** to maintain a live local order book for real-time pricing.

## ▶️ Usage Instructions

Run from the command line:

```bash
python main.py
```

You'll be prompted to input:\
💠 Total BTC amount to purchase\
💠 Duration of the purchase window (in seconds)\
💠 Number of transactions (optional, default is one every 10 seconds)\

## ◀️ Output
In CLI:\
💠 Best price per transaction\
💠 Final total cost after all transactions

Saved Locally:\
💠 data/best_deal.csv – records best price and exchange per transaction\
💠 exchange-specific folders (e.g., data/binance/) store raw JSON snapshots


## 📁 File Structure
```
BTC best price tracker
├── main.py                      # Main execution script
├── data/                        # Saved snapshots and CSV output
│   └── best_deal.csv
├── src/
│   ├── exchanges/               # Exchange-specific API wrappers
│   │   ├── base.py
│   │   ├── binance.py
│   │   ├── coinbase.py
│   │   └── kraken.py
│   └── utils/                   # Utility functions
│       ├── get_best_price.py
│       └── helpers.py
├── .gitignore
├── requirements.txt
└── README.md
```

## 📌 Future Improvements
- Switch to WebSocket streaming for real-time updates
- Add unit tests for core pricing logic
- Better exception handling and retry logic for failed API calls






