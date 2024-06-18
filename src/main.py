"""
main.py

This module orchestrates the process of listing, downloading, processing, and archiving files from Azure Blob Storage and Azure File Share. It also logs the processed files and merges the data into an SQL database.

Functions:
    - list_files_in_blob_storage(container_name: str) -> List[str]: Lists files in the specified Azure Blob Storage container.
    - download_file_from_blob_storage(container_name: str, blob_name: str, download_path: str) -> None: Downloads a specified file from Azure Blob Storage.
    - archive_in_blob(container_name: str, src_blob_name: str, dest_blob_name: str) -> None: Moves a file within Azure Blob Storage.
    - list_files_in_fileshare(fileshare_name: str, directory_path: str = "") -> List[str]: Lists files in the specified Azure File Share directory.
    - download_from_fileshare(local_file_path: str, fileshare_name: str, fileshare_path: str) -> None: Downloads a specified file from Azure File Share.
    - archive_in_fileshare(fileshare_name: str, source_path: str, destination_path: str) -> None: Moves a file within Azure File Share.
    - make_dir(directory_path: str) -> None: Creates a directory if it doesn't already exist.
    - generate_id(length: int = 8) -> str: Generates a random ID of the specified length.
    - filter_files(files: List[str]) -> List[str]: Filters a list of filenames, returning only those that end with .csv, .xls, or .xlsx.
    - process_file(file_path: str) -> pd.DataFrame: Processes the specified file and returns its data as a DataFrame.
    - connection() -> sqlalchemy.engine.base.Connection: Provides a connection to the SQL database.
    - merge_data(source: str, target: str) -> None: Merges data from the source table to the target table in the SQL database.
    - log_file(file_name: str, source: str) -> None: Logs a processed file entry into the SQL database.

Execution:
    - Lists and processes new files from Azure Blob Storage and Azure File Share.
    - Downloads and archives the files.
    - Processes the data and merges it into the SQL database.
    - Logs the processed files.
"""
import os
from blob import (
    list_files_in_blob_storage,
    download_file_from_blob_storage,
    archive_in_blob,
)
from fileshare import (
    list_files_in_fileshare,
    download_from_fileshare,
    archive_in_fileshare,
)
from sql import connection, merge_data, log_file,execute_query
from utils import make_dir, generate_id, filter_files, process_file


if __name__ == "__main__":
    data_changed=False
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    tenant_id = os.environ["AZURE_TENANT_ID"]
    files = list_files_in_blob_storage(container_name="qvh")
    files = [file for file in files if "Processed" not in file]
    files = filter_files(files)
    print(files)
    if files != []:
        previous_data = None
        run_id = generate_id()
        make_dir(run_id)
        for file in files:
            file_name = file.split("/")[-1]
            download_file_from_blob_storage(
                container_name="qvh",
                blob_name=file,
                download_path=f"./{run_id}/{file_name}",
            )
            archive_path = f"home/IQPR/Processed/{file_name}"

            data = process_file(f"./{run_id}/{file_name}")

            data = data.reset_index(drop=False)
            required_columns = {'Metric Name', 'Period', 'Specialty/Trust', 'Numerator', 'Denominator'}
            if not required_columns.issubset(set(data.columns)):
                print("Data does not match the headers, skipping")
            else:
                data=data.drop("Denominator", axis=1)
                data.columns = [
                    "Metric Name",
                    "Period",
                    "Specialty/Trust",
                    "Numerator",
                    "Denominator",
                    "SourceFile",
                ]

                if previous_data is not None and previous_data.equals(data):
                    print(
                        f"File '{file_name}' is identical to the previous file. Skipping SQL write."
                    )
                else:
                    with connection() as conn:
                        assert "Metric Name" in data.columns, "Metric name col is missing"
                        data.to_sql(
                            name="Metrics_Generic",
                            con=conn,
                            schema="staging",
                            if_exists="replace",
                            index=False,
                        )
                        merge_data(source="staging.Metrics_Generic", target="scd.Metric")
                        data_changed=True
                        archive_in_blob(
                container_name="qvh", src_blob_name=file, dest_blob_name=archive_path
            )
                        log_file(file_name=file_name, source="SFTP")
                        previous_data = data

    else:
        print("No new files,skipping...")

    files = list_files_in_fileshare(fileshare_name="qvh", directory_path="Uploads/IQPR")
    files=filter_files(files=files)
    print(files)
    if files != []:
        previous_data = None
        run_id = generate_id()
        make_dir(run_id)
        for file in files:
            print(f"Processing {file}")
            file_name = file.split("/")[-1]
            download_from_fileshare(
                fileshare_name="qvh",
                fileshare_path=f"Uploads/IQPR/{file}",
                local_file_path=f"./{run_id}/{file_name}",
            )
            archive_in_fileshare(
                fileshare_name="qvh",
                source_path=f"Uploads/IQPR/{file}",
                destination_path=f"Uploads/IQPR/Processed",
            )
            data = process_file(f"./{run_id}/{file_name}")
            if previous_data is not None and previous_data.equals(data):
                print(
                    f"File '{file_name}' is identical to the previous file. Skipping SQL write."
                )
            else:
                with connection() as conn:
                    data.to_sql(
                        name="Metrics_ElectiveRecovery",
                        con=conn,
                        schema="staging",
                        if_exists="replace",
                        index=False,
                    )
                data_changed=True
                merge_data(source="staging.Metrics_Generic", target="scd.Metric")
                log_file(file_name=file_name, source="SFTP")
                previous_data = data
    else:
        print("No files,skipping...")


    files = list_files_in_fileshare(fileshare_name="qvh", directory_path="Uploads/IQPR/ElectiveRecovery")
    files=filter_files(files=files)
    print(files)
    if files != []:
        previous_data = None
        run_id = generate_id()
        make_dir(run_id)
        for file in files:
            print(f"Processing {file}")
            file_name = file.split("/")[-1]
            download_from_fileshare(
                fileshare_name="qvh",
                fileshare_path=f"Uploads/IQPR/ElectiveRecovery/{file}",
                local_file_path=f"./{run_id}/{file_name}",
            )
            # archive_in_fileshare(
            #     fileshare_name="qvh",
            #     source_path=f"Uploads/IQPR/ElectiveRecovery/{file}",
            #     destination_path=f"Uploads/IQPR/Processed/{file}",
            # )
            data = process_file(f"./{run_id}/{file_name}")
            if previous_data is not None and previous_data.equals(data):
                print(
                    f"File '{file_name}' is identical to the previous file. Skipping SQL write."
                )
            else:
                with connection() as conn:
                    data.to_sql(
                        name="Metrics_ElectiveRecovery",
                        con=conn,
                        schema="staging",
                        if_exists="replace",
                        index=False,
                    )
                data_changed=True
                # merge_data(source="staging.Metrics_Generic", target="scd.Metric")
                # log_file(file_name=file_name, source="FileShare")
                previous_data = data
    else:
        print("No files,skipping...")

if data_changed is True:
    query = """update scd.RefreshTimes
set UpdateDTTM = getdate()
where Feed = 'Data'"""
    execute_query(query)
else:
    raise ValueError("No new data found")
