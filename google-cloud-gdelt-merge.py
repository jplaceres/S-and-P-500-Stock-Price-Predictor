import os
import pandas as pd
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

"""
THIS CODE SEACHES THROUGH GDELT DATABASE ON GOOGLE CLOUD.
- S&P500 DATASET WAS UPLOADED MANUALLY TO GOOGLE CLOUD STORAGE.
- FOR LOOP WAS USED TO SEARCH MONTH BY MONTH.
    - BIG QUERY WAS USED TO SEARCH THE DATABASE TO OBTAIN DAILY AVERAGE SENTIMENT VALUES (V2Tone on GDELT) FOR EACH COMPANY AND DAILY ARTICLE COUNT.
    - RESULTS WERE SAVED IN A TABLE
- FINAL RESULTS TABLE WAS MERGED WITH THE S&P500 DATASET INTO A NEW TABLE
- TABLE WAS MANUALLY UPLOADED TO LOCAL COMPUTER

MY CREDENTIALS ARE HIDDEN TO AVOID ANY ADDITIONAL COST. HOWEVER, YOU MAY USE THIS CODE ON YOUR OWN ACCOUNT.
"""

# Loading credentials
load_dotenv()
my_project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
client = bigquery.Client(project=my_project_id)
print("Credientials loaded")

# Defining the timeline (GDELT 2.0 has database starting from 2/19/2026)
start_date = pd.to_datetime('2015-02-19')
end_date = pd.to_datetime('2025-12-31')
current_date = start_date

# Saving data inside Google Cloud
destination_table = f"{my_project_id}.sp500_dataset.historical_prices_with_sentiment"

print("Starting Cloud-only GDELT data extraction...")

# Loop through the timeline month by month
while current_date <= end_date:
    next_month = current_date + relativedelta(months=1)
    chunk_end_date = next_month - relativedelta(days=1)
    
    if chunk_end_date > end_date:
        chunk_end_date = end_date

    str_start = current_date.strftime('%Y-%m-%d')
    str_end = chunk_end_date.strftime('%Y-%m-%d')
    
    print(f"Processing data from {str_start} to {str_end}...")

    # Configure the query to save the results directly to the Cloud table
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_APPEND" # Appends new months to the bottom of the table
    )

    sql_query = f"""
    WITH target_month_data AS (
        SELECT 
            Date AS date, 
            Ticker AS ticker, 
            Company AS company_name, 
            Close AS close
        FROM `{my_project_id}.sp500_dataset.historical_prices`
        WHERE CAST(Date AS DATE) BETWEEN '{str_start}' AND '{str_end}'
    ),
    unique_companies AS (
        SELECT DISTINCT company_name FROM target_month_data
    ),
    gdelt_month_matches AS (
        SELECT 
            CAST(PARSE_DATE('%Y%m%d', SUBSTR(CAST(gdelt.DATE AS STRING), 1, 8)) AS DATE) AS publish_date,
            uc.company_name,
            COUNT(gdelt.DocumentIdentifier) AS article_count,
            AVG(CAST(SPLIT(gdelt.V2Tone, ',')[OFFSET(0)] AS FLOAT64)) AS avg_sentiment
        FROM 
            `gdelt-bq.gdeltv2.gkg_partitioned` AS gdelt
        JOIN 
            unique_companies AS uc
        ON 
            REGEXP_CONTAINS(gdelt.V2Organizations, CONCAT(r'\\b', uc.company_name, r'\\b'))
        WHERE 
            _PARTITIONDATE BETWEEN '{str_start}' AND '{str_end}'
            AND gdelt.V2Themes LIKE '%ECON_%'
        GROUP BY 
            publish_date, 
            uc.company_name
    )
    SELECT 
        tmd.date,
        tmd.ticker,
        tmd.company_name,
        tmd.close,
        IFNULL(gmm.article_count, 0) AS article_count,
        gmm.avg_sentiment
    FROM 
        target_month_data AS tmd
    LEFT JOIN 
        gdelt_month_matches AS gmm
    ON 
        CAST(tmd.date AS DATE) = gmm.publish_date 
        AND tmd.company_name = gmm.company_name
    """

    # Run the query and tell it to use the job_config we set up
    query_job = client.query(sql_query, job_config=job_config)
    
    # Python will wait for Google Cloud to finish saving the data before it loops to the next month
    query_job.result() 

    print("Saved chunk directly to BigQuery. Moving to next month...\n")
    current_date = next_month

print("Extraction complete! Your new table is ready in BigQuery.")