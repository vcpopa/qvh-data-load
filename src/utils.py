"""
utils.py

This module provides utility functions for various tasks such as retrieving credentials from Azure KeyVault,
creating directories, generating random IDs, processing files, and filtering file lists based on extensions.

Functions:
    - get_credential(name: str) -> str: Retrieves a credential value from Azure KeyVault.
    - make_dir(directory_path: str) -> None: Creates a directory if it doesn't already exist.
    - generate_id(length: int = 8) -> str: Generates a random ID of the specified length.
    - process_file(file_path: str) -> pd.DataFrame: Processes a file and returns a pandas DataFrame.
    - filter_files(files: List[str]) -> List[str]: Filters a list of filenames based on allowed extensions.
"""

import os
from typing import List
import string
import random
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from exc import KeyVaultError, DataFileError  # pylint: disable=import-error


def get_credential(name: str) -> str:
    """
    Retrieves a credential value from Azure KeyVault

    Parameters:
    name (str): The name of the credential inside KeyVault

    Returns:
    - credential (str)

    Raises:
    - KeyVaultError: If credential is not found or is empty
    """
    kv_uri = "https://qvh-keyvault.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    credential_value = client.get_secret(name).value
    if not credential_value:
        raise KeyVaultError("Credential value not found, please check KeyVault")
    return credential_value


def make_dir(directory_path):
    """
    Creates a directory if it doesn't already exist.

    Parameters:
    - directory_path (str): The path of the directory to create.

    Returns:
    - None
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory {directory_path} created.")
    else:
        print(f"Directory {directory_path} already exists.")


def generate_id(length: int = 8) -> str:
    """
    Generates a random ID of the specified length.

    Parameters:
    - length (int): The length of the random ID to generate. Default is 8.

    Returns:
    - str: The generated random ID.
    """
    characters = string.ascii_letters + string.digits
    random_id = "".join(random.choice(characters) for _ in range(length))
    return random_id


def process_file(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV, XLS, or XLSX file from the specified file path and returns its content as a Pandas DataFrame.
    Adds a 'SourceFile' column to the DataFrame containing the base name of the file.

    Args:
    - file_path (str): The path to the file to be processed.

    Returns:
    - pd.DataFrame: DataFrame containing the data from the file.

    Raises:
    - DataFileError: If the file format is unsupported (not .csv, .xls, or .xlsx).
    """
    if file_path.endswith(".csv"):
        data = pd.read_csv(file_path)
    elif file_path.endswith(".xls") or file_path.endswith(".xlsx"):
        data = pd.read_excel(file_path,engine='openpyxl')
    else:
        raise DataFileError(
            f"{file_path} format is unsupported. Pass in a csv,xls or xlsx file."
        )
    file_name = os.path.basename(file_path)
    data["SourceFile"] = file_name
    return data


def filter_files(files: List[str]) -> List[str]:
    """
    Filters a list of filenames, returning only those that end with .csv, .xls, or .xlsx.

    Args:
        files (List[str]): List of filenames to filter.

    Returns:
        List[str]: Filtered list of filenames.
    """
    allowed_extensions = (".csv", ".xls", ".xlsx")
    return [file for file in files if file.lower().endswith(allowed_extensions)]
