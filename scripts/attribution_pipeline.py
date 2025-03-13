import pandas as pd
import sqlite3
import requests
import json
import time
from tqdm import tqdm
import numpy as np
import os

# Constants for IHC API limits
MAX_SESSIONS_PER_IHC_REQUEST = 200 # We have limit for Free Plan.
MAX_CONVERSIONS_PER_IHC_REQUEST = 100

# Configuration
DB_PATH = 'data/challenge.db'  # Adjust path as needed
IHC_API_KEY = '46544c43-5683-4fde-aff0-66135c712369'  
CONV_TYPE_ID = 'all_markets' 
OUTPUT_PATH = 'channel_reporting.csv'

def extract_customer_journeys():
    """Extract customer journeys from database"""
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
            ROW_NUMBER() OVER (PARTITION BY c.conv_id ORDER BY ss.event_date || ' ' || ss.event_time DESC) as rn,
            c.revenue
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
        CASE WHEN rn = 1 THEN 1 ELSE 0 END AS conversion,
        revenue
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
    
    conn.close()
    return df

def create_chunks_of_customer_journeys(df_cjs):
    """
    Chunks customer journey dataframe into chunks of 200 or fewer sessions.
    Each chunk contains sessions for a single conversion only.
    """
    print("Creating chunks of customer journeys...")
    
    # Group by conversion ID and count sessions
    df_sessions_per_conversion = df_cjs.groupby("conversion_id").size().reset_index(name='session_count')
    
    # Sort conversions by session count (smallest first)
    df_sessions_per_conversion = df_sessions_per_conversion.sort_values('session_count')
    
    # Initialize list to store chunks
    chunks = []
    
    # Process each conversion
    for _, row in df_sessions_per_conversion.iterrows():
        conv_id = row['conversion_id']
        session_count = row['session_count']
        
        # Get all sessions for this conversion
        conv_sessions = df_cjs[df_cjs['conversion_id'] == conv_id]
        
        if session_count <= MAX_SESSIONS_PER_IHC_REQUEST:
            # Conversion fits in one chunk
            chunks.append(conv_sessions.to_dict('records'))
        else:
            # Need to skip or split this conversion
            print(f"Warning: Conversion {conv_id} has {session_count} sessions, which exceeds the limit of {MAX_SESSIONS_PER_IHC_REQUEST}.")
            print(f"This conversion will be excluded.")
    
    print(f"Created {len(chunks)} chunks")
    return chunks

def send_to_ihc_api(chunks):
    """Send chunks to IHC API and collect responses"""
    print(f"Sending {len(chunks)} chunks to IHC API...")
    api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={CONV_TYPE_ID}"
    all_results = []
    
    # Create a directory to store request payloads
    os.makedirs('request_payloads', exist_ok=True)
    
    for i, chunk in enumerate(tqdm(chunks)):
        # Save request payload to file for debugging/Postman
        request_payload = {"customer_journeys": chunk}
        payload_file = f"request_payloads/chunk_{i+1}.json"
        with open(payload_file, 'w') as f:
            json.dump(request_payload, f, indent=2)
        
        print(f"Saved request payload for chunk {i+1} to {payload_file}")
        
        try:
            # Log basic request info
            print(f"\nSending chunk {i+1}/{len(chunks)} to {api_url}")
            print(f"Chunk contains {len(chunk)} touchpoints across {len(set([tp['conversion_id'] for tp in chunk]))} conversions")
            
            # Send request to API
            response = requests.post(
                api_url,
                json=request_payload,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": IHC_API_KEY
                }
            )
            
            # Log response status and basic info
            print(f"Response status code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Error response: {response.text}")
                continue
                
            # Process successful response
            response_data = response.json()
            results = response_data.get("value", [])
            print(f"Received {len(results)} attribution records")
            
            all_results.extend(results)
            
        except Exception as e:
            print(f"Error processing chunk {i+1}: {str(e)}")
            continue
    
    print(f"Received total of {len(all_results)} attribution records from all successful requests")
    return all_results


def write_attribution_results(attribution_data):
    """Write attribution results to database"""
    print("Writing attribution results to database...")
    
    # Convert to DataFrame
    if not attribution_data:
        print("No attribution data to write")
        return
    
    df_attr = pd.DataFrame(attribution_data)
    
    # Rename column if needed to match database schema
    if 'conversion_id' in df_attr.columns and 'conv_id' not in df_attr.columns:
        df_attr = df_attr.rename(columns={'conversion_id': 'conv_id'})
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Write to attribution_customer_journey table
    df_attr[['conv_id', 'session_id', 'ihc']].to_sql(
        'attribution_customer_journey',
        conn,
        if_exists='replace',
        index=False
    )
    
    # Verify IHC values sum to 1 for each conversion
    query = """
    SELECT conv_id, SUM(ihc) as ihc_sum
    FROM attribution_customer_journey
    GROUP BY conv_id
    """
    df_check = pd.read_sql_query(query, conn)
    
    # Check for any significant deviations from 1
    tolerance = 0.01  # Allow for small floating point differences
    invalid_sums = df_check[(df_check['ihc_sum'] < 1 - tolerance) | (df_check['ihc_sum'] > 1 + tolerance)]
    
    if not invalid_sums.empty:
        print(f"Warning: Found {len(invalid_sums)} conversions with IHC sum not equal to 1")
    else:
        print("All conversions have IHC sums of approximately 1 (as expected)")
    
    conn.close()
    print("Attribution results written to database")

def generate_channel_reporting():
    """Generate channel reporting data"""
    print("Generating channel reporting...")
    
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT 
        ss.channel_name,
        ss.event_date as date,
        SUM(sc.cost) as cost,
        SUM(acj.ihc) as ihc,
        SUM(acj.ihc * c.revenue) as ihc_revenue
    FROM 
        session_sources ss
    LEFT JOIN 
        session_costs sc ON ss.session_id = sc.session_id
    LEFT JOIN 
        attribution_customer_journey acj ON ss.session_id = acj.session_id
    LEFT JOIN 
        conversions c ON acj.conv_id = c.conv_id
    GROUP BY 
        ss.channel_name, ss.event_date
    ORDER BY 
        ss.event_date, ss.channel_name
    """
    
    channel_reporting = pd.read_sql_query(query, conn)
    print(f"Generated {len(channel_reporting)} channel reporting records")
    
    # Fill NAs with 0
    channel_reporting = channel_reporting.fillna(0)
    
    # Write to database
    channel_reporting.to_sql('channel_reporting', conn, if_exists='replace', index=False)
    
    conn.close()
    print("Channel reporting written to database")
    
    return channel_reporting


def export_final_report(output_path="channel_reporting.csv"):
    """Export final report with CPO and ROAS"""
    print(f"Exporting final report to {output_path}...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Read channel reporting data
    channel_reporting = pd.read_sql_query("SELECT * FROM channel_reporting", conn)
    conn.close()
    
    if channel_reporting.empty:
        print("No channel reporting data to export")
        return
    
    # Calculate CPO and ROAS
    # CPO = cost / ihc (cost per order)
    channel_reporting['CPO'] = channel_reporting.apply(
        lambda row: row['cost'] / row['ihc'] if row['ihc'] > 0 else np.nan,
        axis=1
    )
    
    # ROAS = ihc_revenue / cost (return on ad spend)
    channel_reporting['ROAS'] = channel_reporting.apply(
        lambda row: row['ihc_revenue'] / row['cost'] if row['cost'] > 0 else np.nan,
        axis=1
    )
    
    # Export to CSV
    channel_reporting.to_csv(output_path, index=False)
    print(f"Exported final report to {output_path}")
    
    return channel_reporting

def run_attribution_pipeline():
    """Run the complete attribution pipeline"""
    print("Starting attribution pipeline...")
    
    # Step 1: Extract customer journeys
    customer_journeys_df = extract_customer_journeys()
    
    
    # Step 2: Chunk data and send to API
    api_chunks = create_chunks_of_customer_journeys(customer_journeys_df)
    api_results = send_to_ihc_api(api_chunks)
    
    # Step 3: Write attribution results to database
    write_attribution_results(api_results)
    
    # Step 4: Generate channel reporting
    generate_channel_reporting()
    
    # Step 5: Export final report with CPO and ROAS
    export_final_report()
    
    print("Attribution pipeline completed successfully!")

if __name__ == "__main__":
    run_attribution_pipeline()