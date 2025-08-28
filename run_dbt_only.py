import os
from dlt.helpers.dbt import create_runner

# Run dbt without dlt pipeline
# This assumes dbt is installed in your current environment
# and profiles.yml is in the same folder

print("Running dbt models standalone...")

runner = create_runner(
    None,  # use current virtual env to run dlt
    None,  # we do not need dataset name and we do not pass any credentials in environment to dlt
    working_dir=".",  # the package below will be cloned to current dir
    package_location=".",  # use local dbt package in current directory
    package_profiles_dir=os.path.abspath("."),  # profiles.yml must be placed in this dir
    package_profile_name="postgres",  # name of the profile
)

print("Starting dbt run...")
models = runner.run_all()

print("\ndbt execution results:")
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f" in {m.time}" +
        f" with status {m.status}" +
        f" and message {m.message}"
    )

print("\nStandalone dbt run completed!")
