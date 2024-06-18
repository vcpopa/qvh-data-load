"""
This module provides a utility for connecting to a SQL Server database using SQLAlchemy
and executing SQL queries to return the results as a Pandas DataFrame. It uses context managers
for managing the database connection and dotenv for loading environment variables.

Functions:
- connection: A context manager for creating and closing the database connection.
- read_sql: Executes a SQL query and returns the result as a Pandas DataFrame.
- execute_query: Executes a SQL command (INSERT, UPDATE, DELETE, MERGE) and commits the transaction.
"""

from contextlib import contextmanager
import urllib
from typing import Iterator, Literal
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from utils import get_credential  # pylint: disable=import-error


@contextmanager
def connection() -> Iterator[Engine]:
    """
    Context manager to create and close a database connection.

    Loads database connection parameters from environment variables, creates
    a SQLAlchemy engine, and yields the engine. The engine is closed when the
    context is exited.

    Returns:
        Iterator[Engine]: An iterator that yields a SQLAlchemy Engine.
    """

    connstr = get_credential("public-dataflow-connectionstring")
    params = urllib.parse.quote_plus(connstr)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    try:
        yield engine
    finally:
        engine.dispose()


def read_sql(query: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a Pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A DataFrame containing the query results.
    """
    with connection() as conn:
        return pd.read_sql(sql=query, con=conn)


def execute_query(query: str) -> None:
    """
    Executes a SQL command (INSERT, UPDATE, DELETE, MERGE) and commits the transaction.

    Args:
        query (str): The SQL command to execute.

    Returns:
        None
    """
    with connection() as conn:
        conn_ = conn.connect()
        with conn_.begin() as transaction:
            try:
                conn_.execute(text(query))
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                raise e


def merge_data(source: str, target: str) -> None:
    """
    Merges data from the specified source table into the target table in the database.

    This function performs a merge operation between a source and target table within a database.
    It uses the `MERGE` SQL statement to update existing records in the target table based on specific
    criteria, or insert new records if they do not already exist. The merge operation considers the
    measure_id, period, and specialty/trust columns to match existing records. If a match is found and
    the numerator or denominator values differ, it updates the records. If no match is found, it inserts
    the new records.

    Args:
    - source (str): The name of the source table containing the data to merge.
    - target (str): The name of the target table where data is merged.

    Returns:
    - None
    """
    query = f"""SET DATEFORMAT DMY
MERGE INTO {target} AS target
USING (
    SELECT measure_id,
        measure_description ,
        CAST([Period]  AS DATE) AS Period,
        [Specialty/Trust],
        [Numerator] as Numerator,
        [Denominator],
        [SourceFile]
   FROM  {source} b inner join scd.measure m on m.measure_description = b.[Metric Name]
    WHERE ISNULL(Numerator,'') <> ''
) AS source
ON target.measure_id = source.measure_id
   AND CAST(target.[Period] AS DATE) = CAST(source.[Period] AS DATE)
   AND target.dim1 = source.[Specialty/Trust]
WHEN MATCHED AND (source.Numerator <> target.Numerator OR isnull(source.denominator,'') <> isnull(target.denominator, '' ) )
THEN
    UPDATE SET
        target.[Numerator] = source.[Numerator],
        target.[Denominator] = source.[Denominator],
        target.[UpdatedBy] = source.[Sourcefile],
        target.[UpdateDTTM] = GETDATE(),
        target.UpdateType ='Updated'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [Measure_ID]
      ,[Period]
      ,DIM1
      ,[Numerator]
      ,[Denominator]
      ,[UpdatedBy]
      ,[UpdateDTTM]
      ,UpdateType
    )
    VALUES (
        source.[Measure_id],
        cast(source.[Period] as date),
        source.[Specialty/Trust],
        source.[Numerator],
        source.[Denominator],
        source.[Sourcefile],
        GETDATE(),
        'Inserted'
    );"""
    execute_query(query)


def log_file(file_name: str, source: Literal["SFTP", "FileShare"]) -> None:
    """
    Logs a file entry into the scd.MetricFileLog table.

    Args:
        file_name (str): The name of the file.
        source (str): The source of the file.

    Returns:
        None
    """
    query = f"""
        INSERT INTO scd.MetricFileLog (FileName, Source, DateUploaded)
        VALUES ({file_name},{source}, GETDATE())
        """
    execute_query(query=query)
