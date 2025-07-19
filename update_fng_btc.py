import requests
import pandas as pd
import os
import time

# Fetch the latest Fear & Greed Index data
def fetch_fng_data():
    for attempt in range(3):  # Retry 3 times
        try:
            r = requests.get('https://api.alternative.me/fng/?limit=1', timeout=10)
            if r.status_code == 200:
                df = pd.DataFrame(r.json()['data'])
                if 'timestamp' not in df.columns:
                    print("Error: 'timestamp' column not found in FNG data")
                    return None
                df['value'] = df['value'].astype(int)
                df['timestamp'] = df['timestamp'].astype(int)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=True)
                return df
            print(f"Attempt {attempt + 1} failed, status code: {r.status_code}, response: {r.text}")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed due to exception: {e}")
        print(f"Waiting 10 seconds before next attempt...")
        time.sleep(10)
    print("Error: Failed to fetch FNG data after 3 attempts")
    return None

# Fetch the latest BTC price data using CoinGecko API
def fetch_btc_data():
    for attempt in range(3):  # Retry 3 times
        try:
            r = requests.get('https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1&interval=daily', timeout=10)
            if r.status_code == 200:
                btc_df = pd.DataFrame(r.json()['prices'], columns=['timestamp', 'BTCUSD'])
                btc_df['timestamp'] = pd.to_datetime(btc_df['timestamp'], unit='ms')
                btc_df.set_index('timestamp', inplace=True)
                return btc_df.iloc[0:1]  # Return only the latest day
            print(f"Attempt {attempt + 1} failed, status code: {r.status_code}, response: {r.text}")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed due to exception: {e}")
        print(f"Waiting 10 seconds before next attempt...")
        time.sleep(10)
    print("Error: Failed to fetch BTC data after 3 attempts")
    return None

# Merge data and upload to Dune
def update_dune_data():
    # Fetch the latest data
    fng_df = fetch_fng_data()
    if fng_df is None:
        return
    btc_df = fetch_btc_data()
    if btc_df is None:
        return

    # Merge data
    d = pd.merge(fng_df, btc_df, how='inner', left_index=True, right_index=True)
    if d.empty:
        print("Error: No data after merge")
        return

    # Read existing CSV to check for duplicates
    output_file = 'fear_greed_btc_data.csv'
    existing_data = []
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_data = existing_df['timestamp'].tolist()
        except Exception as e:
            print(f"Error reading existing CSV: {e}")
            return

    latest_date = d.index[0].strftime('%Y-%m-%d')
    if latest_date in existing_data:
        print(f"Data already exists, skipping upload: {latest_date}")
        return

    # Append to CSV
    d = d.reset_index()
    try:
        d.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
        print(f"Data appended to {output_file}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return

    # Upload to Dune
    api_key = os.environ.get('DUNE_API_KEY')
    if not api_key:
        print("Error: DUNE_API_KEY environment variable not found")
        return
    namespace = 'rekahbeee'
    table_name = 'dataset_fng_btc_data' 
    DUNE_API_URL = f'https://api.dune.com/api/v1/table/{namespace}/{table_name}/insert'
    headers = {
        'X-Dune-API-Key': api_key,
        'Content-Type': 'text/csv' 
    }
    try:
        csv_data = d.to_csv(index=False)
        response = requests.post(DUNE_API_URL, headers=headers, data=csv_data, timeout=10)
        if response.status_code == 200:
            print(f"Data uploaded to Dune successfully! Time: {pd.Timestamp.now()}")
        else:
            print(f"Upload error: Status {response.status_code}, Response: {response.text}")
    except requests.RequestException as e:
        print(f"Upload failed due to exception: {e}")

if __name__ == "__main__":
    update_dune_data()

