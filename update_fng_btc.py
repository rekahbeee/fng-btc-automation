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

# Create Dune table via API (only runs once)
def create_dune_table():
    api_key = os.environ.get('DUNE_API_KEY')
    if not api_key:
        print("Error: DUNE_API_KEY environment variable not found")
        return False
    
    namespace = 'rekahbeee'
    table_name = 'fng_btc_data_api'  # New table name, different from CSV uploaded table
    
    # Define table schema
    schema = [
        {"name": "timestamp", "type": "varchar"},
        {"name": "value", "type": "integer"},
        {"name": "value_classification", "type": "varchar"},
        {"name": "BTCUSD", "type": "double"}
    ]
    
    create_url = 'https://api.dune.com/api/v1/table/create'
    headers = {
        'X-Dune-API-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    payload = {
        "namespace": namespace,
        "table_name": table_name,
        "schema": schema,
        "description": "Fear and Greed Index with BTC price data (API managed)"
    }
    
    try:
        response = requests.post(create_url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"Table created successfully: {namespace}.{table_name}")
            return True
        elif response.status_code == 400 and "already exists" in response.text.lower():
            print(f"Table already exists: {namespace}.{table_name}")
            return True
        else:
            print(f"Table creation error: Status {response.status_code}, Response: {response.text}")
            return False
    except requests.RequestException as e:
        print(f"Table creation failed due to exception: {e}")
        return False

# Import historical data from CSV (only run once manually)
def import_historical_data():
    """
    This function only needs to run once to import existing CSV historical data 
    into the new API table. It's recommended to run this manually first, 
    then comment out this part of the code.
    """
    output_file = 'fear_greed_btc_data.csv'
    if not os.path.exists(output_file):
        print(f"Historical data file not found: {output_file}")
        return
    
    try:
        historical_df = pd.read_csv(output_file)
        print(f"Found {len(historical_df)} historical records")
        
        # Upload historical data to Dune
        api_key = os.environ.get('DUNE_API_KEY')
        namespace = 'rekahbeee'
        table_name = 'fng_btc_data_api'
        DUNE_API_URL = f'https://api.dune.com/api/v1/table/{namespace}/{table_name}/insert'
        
        headers = {
            'X-Dune-API-Key': api_key,
            'Content-Type': 'application/json'
        }
        
        # Upload historical data in batches (max 1000 records per batch)
        batch_size = 1000
        for i in range(0, len(historical_df), batch_size):
            batch_df = historical_df.iloc[i:i+batch_size]
            data_records = batch_df.to_dict('records')
            payload = {"data": data_records}
            
            try:
                response = requests.post(DUNE_API_URL, headers=headers, json=payload, timeout=30)
                if response.status_code == 200:
                    print(f"Batch {i//batch_size + 1} uploaded successfully ({len(batch_df)} records)")
                else:
                    print(f"Batch {i//batch_size + 1} upload error: Status {response.status_code}, Response: {response.text}")
            except requests.RequestException as e:
                print(f"Batch {i//batch_size + 1} upload failed: {e}")
            
            # Avoid API rate limits
            time.sleep(1)
            
    except Exception as e:
        print(f"Error importing historical data: {e}")

# Check if data already exists in Dune table
def check_existing_data_in_dune(timestamp_str):
    """
    Check if data for a specific date already exists in the Dune table
    Note: This requires query API implementation, simplified for now
    """
    # This can be implemented with query logic, for now returns False to always try inserting
    return False

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

    # Save to local CSV as backup
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
        print(f"Data already exists in local CSV, skipping: {latest_date}")
        return

    # Append to local CSV
    d_reset = d.reset_index()
    try:
        d_reset.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
        print(f"Data appended to {output_file}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return

    # Create/verify Dune table
    if not create_dune_table():
        print("Failed to create/verify Dune table")
        return
    
    # Upload to Dune API table
    api_key = os.environ.get('DUNE_API_KEY')
    namespace = 'rekahbeee'
    table_name = 'fng_btc_data_api'  # Use new API table name
    DUNE_API_URL = f'https://api.dune.com/api/v1/table/{namespace}/{table_name}/insert'
    
    headers = {
        'X-Dune-API-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    # Convert data to JSON format
    data_records = d_reset.to_dict('records')
    payload = {"data": data_records}
    
    try:
        response = requests.post(DUNE_API_URL, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"Data uploaded to Dune API table successfully! Time: {pd.Timestamp.now()}")
        else:
            print(f"Upload error: Status {response.status_code}, Response: {response.text}")
    except requests.RequestException as e:
        print(f"Upload failed due to exception: {e}")

if __name__ == "__main__":
    # Check if historical data import is needed
    if os.environ.get('IMPORT_HISTORICAL') == 'true':
        print("Importing historical data...")
        import_historical_data()
    
    # Daily update
    update_dune_data()