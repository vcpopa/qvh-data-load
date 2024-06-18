"""
blob.py

This module provides functions to interact with Azure Blob Storage, including listing files, downloading files, and moving files within Blob Storage.

Functions:
    - get_blob_service_client() -> BlobServiceClient: Creates and returns a BlobServiceClient.
    - list_files_in_blob_storage(container_name: str) -> List[str]: Lists all files in a specified container.
    - download_file_from_blob_storage(container_name: str, blob_name: str, download_path: str) -> None: Downloads a file from a specified container.
    - archive_in_blob(container_name: str, src_blob_name: str, dest_blob_name: str) -> None: Moves a file within Azure Blob Storage.
"""

import os
from typing import List
from azure.storage.blob import BlobServiceClient


def get_blob_service_client() -> BlobServiceClient:
    """
    Creates a BlobServiceClient to interact with the Azure Blob Storage service.

    Returns:
        BlobServiceClient: An instance of BlobServiceClient.
    """
    account_name = os.environ["BLOB_ACCOUNT"]
    account_key = os.environ["BLOB_KEY"]
    blob_service_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    return blob_service_client


def list_files_in_blob_storage(container_name: str) -> List[str]:
    """
    Lists all files (blobs) inside a specified container in Azure Blob Storage.

    Parameters:
    - container_name: Name of the container to list files from

    Returns:
    - List[str]: List of filenames sorted by last modified date from oldest to newest
    """

    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs()

    files_in_container = []
    for blob in blob_list:
        files_in_container.append((blob.name, blob.last_modified))

    # Sort files by last modified date from oldest to newest
    files_in_container.sort(key=lambda x: x[1])

    # Extract only the filenames, discarding the last modified date
    sorted_files = [file[0] for file in files_in_container]

    return sorted_files


def download_file_from_blob_storage(
    container_name: str, blob_name: str, download_path: str
) -> None:
    """
    Downloads a file (blob) from a specified container in Azure Blob Storage.

    Parameters:
    - container_name: Name of the container containing the blob
    - blob_name: Name of the blob (file) to download
    - download_path: Local path where the file should be downloaded to
    """

    blob_service_client = get_blob_service_client()

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Get the blob client for the specified blob
    blob_client = container_client.get_blob_client(blob_name)

    # Download the blob to a local file
    with open(download_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    print(f"File '{blob_name}' downloaded successfully to '{download_path}'")


def archive_in_blob(container_name: str, src_blob_name: str, dest_blob_name: str) -> None:
    """
    Moves a file (blob) from one location to another within Azure Blob Storage.

    Parameters:
    - src_container_name: Name of the source container
    - src_blob_name: Name of the source blob (file)
    - dest_container_name: Name of the destination container
    - dest_blob_name: Name of the destination blob (file)
    """

    blob_service_client = get_blob_service_client()

    # Get the source and destination container clients
    container_client = blob_service_client.get_container_client(container_name)

    # Get the blob client for the source blob
    src_blob_client = container_client.get_blob_client(src_blob_name)

    # Get the blob client for the destination blob
    dest_blob_client = container_client.get_blob_client(dest_blob_name)

    # Copy the source blob to the destination
    copy_source = src_blob_client.url
    dest_blob_client.start_copy_from_url(copy_source)

    # Wait for the copy operation to complete
    copy_status = dest_blob_client.get_blob_properties().copy.status
    while copy_status == "pending":
        copy_status = dest_blob_client.get_blob_properties().copy.status

    # Delete the source blob
    src_blob_client.delete_blob()

    print(f"File '{src_blob_name}' moved successfully to '{container_name}/{dest_blob_name}'")
