import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.common.configuration import configspec
from typing import Any, Dict, Optional
from base64 import b64encode
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

tickers = ["IBM", "AAPL"]


config: RESTAPIConfig = {
    "client": {
        "base_url": "https://backend.simfin.com/api/v3/",
        "headers": {
            "Authorization": dlt.secrets["sources.simfin.access_token"],
        },
        "paginator": SinglePagePaginator() # <---- set up paginator type
    },
    "resource_defaults": {
        "max_table_nesting": 0 # Set your desired nesting level here
    },
    "resources": [  # <--- list resources
        {
          "name": "price",
          "endpoint": {
              "path": "companies/prices/verbose",
              "params": {
                    "ticker": ",".join(tickers),
                    "start": "{incremental.start_value}",  # Uses cursor value in query parameter
                        },
                "incremental": {
                        "cursor_path": "data[*].Date",
                        "initial_value": "2010-08-30",
                        },
                },
                },
        #{
        #  "name": "company_info",
        #  "endpoint": {
        #      "path": "companies/general/verbose",
        #      "params": {
        #            "ticker": ",".join(tickers),
        #            },
        #            #"paginator": QueryParamPaginator()
        #            },
        #},
    ],
}

simfin_source_raw = rest_api_source(config)

@dlt.transformer(data_from=simfin_source_raw.price, table_name="price", max_table_nesting=0)
def transformed_price(items):
    for item in items:
        # Extract company metadata
        company_id = item['id']
        company_name = item['name']
        ticker = item['ticker']
        currency = item['currency']
        isin = item['isin']
        
        # Process each daily data point
        for daily_data in item['data']:
            new_mapping = {}
            new_mapping['company_id'] = company_id
            new_mapping['company_name'] = company_name
            new_mapping['ticker'] = ticker
            new_mapping['currency'] = currency
            new_mapping['isin'] = isin
            new_mapping['date'] = daily_data['Date']
            new_mapping['dividend_paid'] = daily_data['Dividend Paid']
            new_mapping['common_shares_outstanding'] = daily_data['Common Shares Outstanding']
            new_mapping['last_closing_price'] = daily_data['Last Closing Price']
            new_mapping['adjusted_closing_price'] = daily_data['Adjusted Closing Price']
            new_mapping['highest_price'] = daily_data['Highest Price']
            new_mapping['lowest_price'] = daily_data['Lowest Price']
            new_mapping['opening_price'] = daily_data['Opening Price']
            new_mapping['trading_volume'] = daily_data['Trading Volume']
            
            yield new_mapping

@dlt.source
def simfin_source():
    return transformed_price

# First pipeline: Load raw data to simfin_raw dataset
pipeline_raw = dlt.pipeline(
    export_schema_path="simfin_schemas/export",
    pipeline_name="rest_api_simfin",
    destination="postgres",
    dataset_name="simfin_raw",
    dev_mode=False,
)

# Run the raw data pipeline
load_info = pipeline_raw.run(simfin_source())
print("Raw data load info:")
print(load_info)
print("Raw data preview:")
print(pipeline_raw.dataset().price.df())

# Second pipeline: Create transformation dataset for dbt
pipeline_dbt = dlt.pipeline(
    export_schema_path="simfin_schemas/export",
    pipeline_name="rest_api_simfin",
    destination="postgres",
    dataset_name="simfin_dbt",
    dev_mode=False,
)

# Make or restore venv for dbt, using the latest dbt version
# NOTE: If you have dbt installed in your current environment, just skip this line
#       and the `venv` argument to dlt.dbt.package()
#venv = dlt.dbt.get_venv(pipeline_dbt)

# Get runner, optionally pass the venv
dbt = dlt.dbt.package(
    pipeline_dbt,
    ".",  # Use local dbt package in current directory
    #venv=venv
)

# Run the models and collect any info
# If running fails, the error will be raised with a full stack trace
print("\nRunning dbt models...")
models = dbt.run_all()

# On success, print the outcome
print("\ndbt execution results:")
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f" in {m.time}" +
        f" with status {m.status}" +
        f" and message {m.message}"
    )

print("\nPipeline completed successfully!")