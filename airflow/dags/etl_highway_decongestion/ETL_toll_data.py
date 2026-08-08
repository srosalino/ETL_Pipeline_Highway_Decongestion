# Import the necessary libraries
import csv
import logging
import tarfile
import urllib.request

from airflow import DAG
from datetime import datetime, timedelta
from pathlib import Path
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def task_failure_callback(context):
    """
    Log useful information when an Airflow task fails.
    """

    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = task_instance.dag_id if task_instance else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    run_id = dag_run.run_id if dag_run else "unknown"

    logging.error(
        "Task failure detected. "
        "DAG: %s | Task: %s | Run: %s | Exception: %r",
        dag_id,
        task_id,
        run_id,
        exception,
    )


# Set the DAG arguments
default_args = {

    "owner": "sebastiao_rosalino",
    "start_date": datetime(2026, 1, 1),

    # Retry settings
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),

    # Failure handling
    "on_failure_callback": task_failure_callback,
}


# Create the DAG
dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="ETL Pipeline for Highway Decongestion",
    schedule_interval="@daily",
)


# ---------------------------------------------------------
# 0. Locations
# ---------------------------------------------------------

DATASET_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)

STAGING_DIR = Path("/home/project/airflow/dags/etl_highway_decongestion/staging")

DESTINATION_PATH = STAGING_DIR / "tolldata.tgz"


# ---------------------------------------------------------
# 1. Download the dataset
# ---------------------------------------------------------

def download_dataset():
    """
    Download tolldata.tgz into the staging directory.
    """

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    urllib.request.urlretrieve(
        DATASET_URL,
        DESTINATION_PATH,
    )

    print(f"Dataset downloaded to: {DESTINATION_PATH}")


# ---------------------------------------------------------
# 2. Extract the .tgz archive
# ---------------------------------------------------------

def untar_dataset():
    """
    Extract tolldata.tgz into the staging directory.
    """

    if not DESTINATION_PATH.exists():
        raise FileNotFoundError(
            f"Destination does not exist: {DESTINATION_PATH}"
        )

    with tarfile.open(DESTINATION_PATH, mode="r:gz") as archive:
        archive.extractall(path=STAGING_DIR)

    print(f"Dataset extracted into: {STAGING_DIR}")


# ---------------------------------------------------------
# 3. Validate that all the necessary files are present
# ---------------------------------------------------------

def validate_extracted_files():
    
    expected_files = [
        STAGING_DIR / "vehicle-data.csv",
        STAGING_DIR / "tollplaza-data.tsv",
        STAGING_DIR / "payment-data.txt",
    ]

    missing_files = [
        path for path in expected_files
        if not path.exists()
    ]

    if missing_files:
        raise FileNotFoundError(
            f"Missing extracted files: {missing_files}"
        )

    print("All expected extracted files are present.")


# ---------------------------------------------------------
# 4. Extract columns from vehicle-data.csv
# ---------------------------------------------------------

def extract_data_from_csv():
    """
    Extract these fields from vehicle-data.csv:

    1. Rowid
    2. Timestamp
    3. Anonymized Vehicle number
    4. Vehicle type

    Save them to csv_data.csv.
    """

    source_file = STAGING_DIR / "vehicle-data.csv"
    destination_file = STAGING_DIR / "csv_data.csv"

    with source_file.open("r", newline="", encoding="utf-8") as source:
        
        reader = csv.reader(source)

        with destination_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as destination:
            
            writer = csv.writer(destination)

            for row in reader:
                if not row:
                    continue

                # Keep columns 0 through 3
                writer.writerow(row[0:4])

    print(f"CSV data saved to: {destination_file}")


# ---------------------------------------------------------
# 5. Extract columns from tollplaza-data.tsv
# ---------------------------------------------------------

def extract_data_from_tsv():
    """
    Extract these fields from tollplaza-data.tsv:

    5. Number of axles
    6. Tollplaza ID
    7. Tollplaza code

    Save them as comma-separated data in tsv_data.csv.
    """

    source_file = STAGING_DIR / "tollplaza-data.tsv"
    destination_file = STAGING_DIR / "tsv_data.csv"

    with source_file.open("r", newline="", encoding="utf-8") as source:
        
        reader = csv.reader(source, delimiter="\t")

        with destination_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as destination:
            
            writer = csv.writer(destination)

            for row in reader:
                
                if not row:
                    continue

                # Keep columns 4 through 6
                writer.writerow(row[4:7])

    print(f"TSV data saved to: {destination_file}")


# ---------------------------------------------------------
# 6. Extract columns from payment-data.txt
# ---------------------------------------------------------

def extract_data_from_fixed_width():
    """
    Extract Type of Payment code and Vehicle Code from the
    fixed-width payment-data.txt file.

    The required fields start at character position 58.
    Save them to fixed_width_data.csv.
    """

    source_file = STAGING_DIR / "payment-data.txt"
    destination_file = STAGING_DIR / "fixed_width_data.csv"

    with source_file.open("r", encoding="utf-8") as source:
        
        with destination_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as destination:
            
            writer = csv.writer(destination)

            for line in source:
                
                if not line.strip():
                    continue

                # Extract the required part consisting of the two fields starting at position 58
                required_section = line[58:].strip()

                # Separate the two fields using whitespace
                fields = required_section.split()

                if len(fields) != 2:
                    raise ValueError(
                        f"Could not parse fixed-width row (the two fields were not detected or more than 2 fields were detected): {line!r}"
                    )

                payment_code = fields[0]
                vehicle_code = fields[1]

                writer.writerow([payment_code, vehicle_code])

    print(f"Fixed-width data saved to: {destination_file}")


# ---------------------------------------------------------
# 7. Consolidate the three extracted files
# ---------------------------------------------------------

def consolidate_data():
    """
    Combine csv_data.csv, tsv_data.csv and fixed_width_data.csv, row by row into extracted_data.csv.
    """

    csv_file = STAGING_DIR / "csv_data.csv"
    tsv_file = STAGING_DIR / "tsv_data.csv"
    fixed_width_file = STAGING_DIR / "fixed_width_data.csv"
    
    destination_file = STAGING_DIR / "extracted_data.csv"

    with (
        csv_file.open("r", newline="", encoding="utf-8") as csv_source,
        tsv_file.open("r", newline="", encoding="utf-8") as tsv_source,
        fixed_width_file.open(
            "r",
            newline="",
            encoding="utf-8",
        ) as fixed_source,
        
        destination_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as destination,
    ):
        csv_reader = csv.reader(csv_source)
        tsv_reader = csv.reader(tsv_source)
        fixed_reader = csv.reader(fixed_source)
        
        writer = csv.writer(destination)

        for csv_row, tsv_row, fixed_row in zip(
            csv_reader,
            tsv_reader,
            fixed_reader,
        ):
            consolidated_row = csv_row + tsv_row + fixed_row
            writer.writerow(consolidated_row)

    print(f"Consolidated data saved to: {destination_file}")


# ---------------------------------------------------------
# 8. Validate the consolidated data
# ---------------------------------------------------------

def validate_consolidated_data():

    source_file = STAGING_DIR / "extracted_data.csv"

    row_count = 0

    with source_file.open(
        mode="r",
        newline="",
        encoding="utf-8",
    ) as file:
        
        reader = csv.reader(file)

        for line_number, row in enumerate(reader, start=1):

            if len(row) != 9:

                raise ValueError(
                    f"Line {line_number}: expected exactly 9 columns, "
                    f"but found {len(row)}: {row!r}"
                )

            row_count += 1

    if row_count == 0:
        raise ValueError("The consolidated file contains no records.")

    print(
        f"Consolidated data passed validation: "
        f"{row_count} records."
    )


# ---------------------------------------------------------
# 9. Transform vehicle type to uppercase
# ---------------------------------------------------------

def transform_data():
    """
    Convert the Vehicle type field to uppercase and save the
    result to transformed_data.csv.
    """

    source_file = STAGING_DIR / "extracted_data.csv"
    destination_file = STAGING_DIR / "transformed_data.csv"

    with source_file.open("r", newline="", encoding="utf-8") as source:
        
        reader = csv.reader(source)

        with destination_file.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as destination:
            
            writer = csv.writer(destination)

            for row in reader:
                
                if not row:
                    continue

                if len(row) != 9:
                    raise ValueError(
                        f"Expected 9 columns, but found {len(row)}: {row}"
                    )

                # Vehicle type is field 3
                row[3] = row[3].upper()

                writer.writerow(row)

    print(f"Transformed data saved to: {destination_file}")


# ---------------------------------------------------------
# 10. Create the tasks
# ---------------------------------------------------------

download_dataset_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag,
)

unzip_dataset_task = PythonOperator(
    task_id="untar_dataset",
    python_callable=untar_dataset,
    dag=dag,
)

validate_extracted_files_task = PythonOperator(
    task_id="validate_extracted_files",
    python_callable=validate_extracted_files,
    dag=dag
)

with TaskGroup(
    group_id="extract_source_data",
    tooltip="Extract the required fields from the three source files",
    dag=dag,
) as extraction_group:

    extract_data_from_csv_task = PythonOperator(
        task_id="extract_data_from_csv",
        python_callable=extract_data_from_csv,
    )

    extract_data_from_tsv_task = PythonOperator(
        task_id="extract_data_from_tsv",
        python_callable=extract_data_from_tsv,
    )

    extract_data_from_fixed_width_task = PythonOperator(
        task_id="extract_data_from_fixed_width",
        python_callable=extract_data_from_fixed_width,
    )

consolidate_data_task = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)

validate_consolidated_data_task = PythonOperator(
    task_id="validate_consolidated_data",
    python_callable=validate_consolidated_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)   


# ---------------------------------------------------------
# 11. Set the pipeline dependencies
# ---------------------------------------------------------

(
    download_dataset_task
    >> unzip_dataset_task
    >> validate_extracted_files_task
    >> extraction_group
    >> consolidate_data_task
    >> validate_consolidated_data_task
    >> transform_data_task
)
