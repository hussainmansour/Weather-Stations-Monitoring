import pandas as pd
import json
import requests

# === CONFIGURATION ===
parquet_file = "/media/hussein/Data/data/Github Repos/Weather-Stations-Monitoring/elastic-kibana/weather-analysis/parquet/1.parquet"  # Path to input Parquet file
es_url = "http://localhost:9200/_bulk"            # Elasticsearch bulk API URL
index_name = "weather"                            # Elasticsearch target index name

# === LOAD AND FLATTEN PARQUET FILE ===
df = pd.read_parquet(parquet_file)

# Flatten nested 'weather' dictionary column if it exists
if 'weather' in df.columns:
    weather_df = df['weather'].apply(pd.Series)
    df = pd.concat([df.drop(columns=['weather']), weather_df], axis=1)

# === CONVERT TO NDJSON FORMAT FOR BULK INDEXING ===
ndjson_lines = []
for _, row in df.iterrows():
    meta = {"index": {"_index": index_name}}
    doc = row.to_dict()
    ndjson_lines.append(json.dumps(meta))
    ndjson_lines.append(json.dumps(doc))

ndjson_payload = "\n".join(ndjson_lines) + "\n"

# === SEND BULK REQUEST TO ELASTICSEARCH ===
try:
    response = requests.post(
        es_url,
        headers={"Content-Type": "application/x-ndjson"},
        data=ndjson_payload.encode('utf-8')
    )

    if response.status_code == 200:
        result = response.json()
        if result.get("errors"):
            print("Some documents failed to index.")
        else:
            print("All documents indexed successfully.")
    else:
        print(f"Failed to index documents. HTTP status code: {response.status_code}")
        print(response.text)

except requests.exceptions.RequestException as e:
    print("Failed to connect to Elasticsearch.")
    print(str(e))