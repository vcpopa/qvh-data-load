"""
fileshare.py

This module provides functions to interact with Azure File Share, including listing files, downloading files, and moving files within the file share.

Functions:
    - get_fileshare_service_client() -> ShareServiceClient: Creates and returns a ShareServiceClient.
    - list_files_in_fileshare(fileshare_name: str, directory_path: str = "") -> List[str]: Lists all files in a specified directory.
    - download_from_fileshare(local_file_path: str, fileshare_name: str, fileshare_path: str) -> None: Downloads a file from a specified path.
    - archive_in_fileshare(fileshare_name: str, source_path: str, destination_path: str) -> None: Moves a file within Azure File Share.
"""

import os
from typing import List
from azure.storage.fileshare import ShareServiceClient
from exc import FileShareError


def get_fileshare_service_client() -> ShareServiceClient:
    """
    Creates a ShareServiceClient to interact with the Azure File Share service.

    Returns:
        ShareServiceClient: An instance of ShareServiceClient.
    """
    account_name = os.environ["FILESHARE_ACCOUNT"]
    account_key = os.environ["FILESHARE_KEY"]
    service_client = ShareServiceClient(
        account_url=f"https://{account_name}.file.core.windows.net",
        credential=account_key,
    )
    return service_client


def list_files_in_fileshare(fileshare_name: str, directory_path: str = "") -> List[str]:
    """
    Lists all files in a specified directory of an Azure File Share.

    Args:
        fileshare_name (str): The name of the Azure File Share.
        directory_path (str, optional): The path within the file share directory to list files from. Defaults to the root directory.

    Returns:
        List[str]: A list of file names in the specified directory of the Azure File Share.
    """
    service_client = get_fileshare_service_client()
    share_client = service_client.get_share_client(fileshare_name)
    directory_client = share_client.get_directory_client(directory_path)
    file_list = directory_client.list_directories_and_files()

    files_in_directory = []
    for file_or_dir in file_list:
        if not file_or_dir["is_directory"]:
            file_properties = directory_client.get_file_client(
                file_or_dir["name"]
            ).get_file_properties()
            files_in_directory.append(
                (file_or_dir["name"], file_properties["last_modified"])
            )

    # Sort files by last modified date from oldest to newest
    files_in_directory.sort(key=lambda x: x[1])

    # Extract only the filenames, discarding the last modified date
    sorted_files = [file[0] for file in files_in_directory]

    return sorted_files


def download_from_fileshare(
    local_file_path: str, fileshare_name: str, fileshare_path: str
) -> None:
    """
    Downloads a file from a specified path in an Azure File Share to a local path.

    Args:
        local_file_path (str): The local path where the file will be saved.
        fileshare_name (str): The name of the Azure File Share.
        fileshare_path (str): The path within the Azure File Share from where the file will be downloaded.

    Returns:
        None
    """
    service_client = get_fileshare_service_client()
    file_client = service_client.get_share_client(fileshare_name).get_file_client(
        fileshare_path
    )
    with open(local_file_path, "wb") as target_file:
        data = file_client.download_file()
        data.readinto(target_file)


def archive_in_fileshare(
    fileshare_name: str, source_path: str, destination_path: str
) -> None:
    """
    Moves a file from one directory to another within the same Azure File Share.

    Args:
        fileshare_name (str): The name of the Azure File Share.
        source_path (str): The source path of the file within the Azure File Share.
        destination_path (str): The destination path within the Azure File Share where the file will be moved.

    Returns:
        None
    """
    service_client = get_fileshare_service_client()
    share_client = service_client.get_share_client(fileshare_name)
    source_file_client = share_client.get_file_client(source_path)

    # Extract the file name from the source path
    file_name = os.path.basename(source_path)
    print("Archiving {source_path} to {destination_path}")
    # Create a new file client for the destination
    destination_file_client = share_client.get_file_client(
        os.path.join(destination_path, file_name)
    )

    _ = destination_file_client.start_copy_from_url(source_file_client.url)

    # Wait for the copy to complete
    properties = destination_file_client.get_file_properties()
    while properties["copy"]["status"] == "pending":
        properties = destination_file_client.get_file_properties()

    # Ensure the copy succeeded
    if properties["copy"]["status"] != "success":
        raise FileShareError(f"{source_path} archiving failed")

    # If copy was successful, delete the original file
    source_file_client.delete_file()
