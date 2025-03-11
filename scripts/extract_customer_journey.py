import sqlite3
import json
import pandas as pd

# Configuration
DB_PATH = 'data/challenge.db'
OUTPUT_JSON = 'customer_journeys.json'

def extract_customer_journeys():
    """Extract customer journeys from database and format as JSON"""
    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    
    print("Extracting customer journeys...")
    query = """
    WITH journeys AS (
        SELECT 
            c.conv_id AS conversion_id,
            ss.session_id,
            ss.event_date || ' ' || ss.event_time AS timestamp,
            ss.channel_name AS channel_label,
            ss.holder_engagement,
            ss.closer_engagement,
            ss.impression_interaction,
            ROW_NUMBER() OVER (PARTITION BY c.conv_id ORDER BY ss.event_date || ' ' || ss.event_time DESC) as rn
        FROM 
            conversions c
        JOIN 
            session_sources ss ON c.user_id = ss.user_id
        WHERE 
            ss.event_date || ' ' || ss.event_time <= c.conv_date || ' ' || c.conv_time
    )
    SELECT
        conversion_id,
        session_id,
        timestamp,
        channel_label,
        holder_engagement,
        closer_engagement,
        impression_interaction,
        CASE WHEN rn = 1 THEN 1 ELSE 0 END AS conversion
    FROM journeys
    ORDER BY conversion_id, timestamp
    """
    
    # Execute query and load into DataFrame
    df = pd.read_sql_query(query, conn)
    print(f"Extracted {len(df)} touchpoints for {df['conversion_id'].nunique()} conversions")
    
    # Clean the data
    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    
    # Convert engagement columns to integers
    for col in ['holder_engagement', 'closer_engagement', 'impression_interaction', 'conversion']:
        df[col] = df[col].astype(int)
    
    # Convert DataFrame to list of dictionaries (JSON format)
    journeys = df.to_dict(orient='records')
    
    # Save to JSON file
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(journeys, f, indent=4)
    
    print(f"Saved {len(journeys)} touchpoints to {OUTPUT_JSON}")
    
    # Close database connection
    conn.close()
    
    return journeys

if __name__ == "__main__":
    extract_customer_journeys()