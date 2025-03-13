# Attribution Pipeline Orchestration

## Overview

This project implements a data pipeline for calculating marketing attribution using the IHC (Initializer-Holder-Closer) attribution model. The pipeline extracts customer journey data from a SQLite database, sends it to the IHC Attribution API, processes the attribution values, and generates reporting data.

## Features

* Extracts customer journeys by joining session and conversion data
* Chunks data to respect API limits for the IHC free plan (200 sessions per request)
* Sends journey data to IHC API for attribution calculation
* Writes attribution results to database
* Generates channel reporting with marketing effectiveness metrics
* Exports final report with CPO (Cost Per Order) and ROAS (Return On Ad Spend)
* Supports time range filtering for selective data processing

## Pre-requisites

* Python 3.6 or higher
* SQLite database with the required tables
* An IHC Attribution API key (free tier account)

## Installation

1. Clone the repository or download the source code
2. Install required dependencies:
```bash
pip install pandas numpy requests tqdm
```
3. Make sure you have the SQLite database file (`challenge.db`) in the project directory

## Database schema:

The pipeline works with the following database tables:

* **session_sources:** Contains information about user sessions including channel, date/time, and engagement flags
* **conversions:** Records conversion events with revenue information
* **session_costs:** Tracks marketing costs per session
* **attribution_customer_journey:** Stores the attribution values from the IHC API
* **channel_reporting:** Stores aggregated channel performance data

## Usage

1. Place the `challenge.zip` file from the task in the `data/` directory
2. Extract the database using:
```bash
python3 scripts/extract_zip.py
```
This will extract `challenge.db` into the data folder, which will be used by the pipeline scripts

3. Run the pipeline with:
```bash
python3 attribution_pipeline.py
```

To process data for a specific time range:
```bash
python3 attribution_pipeline.py --start_date 2025-01-01 --end_date 2025-01-31
```

## Pipeline Steps

1. **Data Extraction:** Queries the database to build customer journeys by joining conversions with session_sources
2. **Data Chunking:** Splits data into chunks that respect the IHC API limits
3. **API Processing:** Sends journey data to the IHC API and retrieves attribution values
4. **Database Update:** Writes attribution results to the attribution_customer_journey table
5. **Reporting:** Generates channel_reporting data and calculates CPO and ROAS metrics
6. **Export:** Creates a CSV file with the final report

## API Limits

The free tier of the IHC API has the following limits:
* Maximum 200 sessions per request
* Each conversion's sessions must be processed in a single request


## Output

The pipeline produces a CSV file (`channel_reporting.csv`) with the following columns:

* channel_name: Marketing channel
* date: Session date
* cost: Sum of marketing costs
* ihc: Sum of attribution values
* ihc_revenue: Sum of attributed revenue
* CPO: Cost Per Order (cost ÷ ihc)
* ROAS: Return On Ad Spend (ihc_revenue ÷ cost)

## Debug Information

The pipeline saves API request payloads to the `request_payloads` directory for debugging purposes. These files can be used to test API calls using tools like Postman.

## Design, Assumptions, and Potential Improvements

## Pipeline Design

The pipeline was designed with the following principles in mind:

1. **Modularity:** Each step of the pipeline is implemented as a separate function, making it easy to test, debug, and maintain.
2. **Efficiency:** The pipeline uses SQL to handle data joining and filtering at the database level, which is more efficient than retrieving all data and processing it in memory.
3. **Robustness:** The pipeline includes error handling, validation checks (such as verifying that IHC values sum to 1), and detailed logging.
4. **Flexibility:** The implementation supports time range filtering to allow processing specific periods of data.

## Assumptions

Several assumptions were made during development:

1. **Data Quality:** The pipeline assumes that user IDs are consistent between conversions and sessions, and that timestamps are in a format that SQLite can compare.
2. **Attribution Model:** It's assumed that all sessions before a conversion can contribute to that conversion, and that all touchpoints for a conversion must be processed together.
3. **Database Schema:** The pipeline assumes that the required tables exist with the specified schema, particularly that session_sources and conversions have compatible user IDs.
4. **API Behavior:** The implementation assumes that the IHC API will correctly distribute attribution values that sum to 1 for each conversion.
5. **Free Tier Limitations:** The pipeline is designed to work within the 200 session limit of the free tier, excluding conversions with more touchpoints.

## Potential Improvements

The pipeline could be enhanced in several ways:

1. **Parallelization:** API requests could be parallelized to improve performance, especially when processing many small chunks.
2. **Incremental Processing:** The pipeline could be modified to only process new or changed data since the last run.
3. **Advanced Chunking:** For paid API tiers with higher limits, the chunking algorithm could be optimized to maximize throughput.
4. **Error Recovery:** Implementation of checkpointing and retry logic would make the pipeline more resilient to temporary failures.
5. **Visualization:** Adding data visualization of the attribution results would provide better insights into marketing effectiveness.
6. **Integration with Orchestration Tools:** The pipeline could be adapted to work with tools like Airflow for scheduling and monitoring.
7. **Handling Large Conversions:** For conversions with more than 200 sessions (exceeding free tier limits), a strategy could be developed to approximate attribution values.
8. **Performance Optimization:** Database indices could be added to improve query performance, especially for large datasets.

## Limitations

* Conversions with more than 200 sessions are excluded due to free tier API limits
* The pipeline assumes that the required database tables exist with the specified schema

## Contributors

This project was created as a solution for a data engineering test challenge.
