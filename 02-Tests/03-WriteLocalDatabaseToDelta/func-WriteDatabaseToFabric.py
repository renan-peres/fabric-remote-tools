# Enviroment/System Management
import os
from dotenv import load_dotenv
import logging
import tempfile
import concurrent.futures
from typing import Generator, Union, Optional, List
from datetime import datetime
import time
import urllib

# DataFrames
import pandas as pd
import polars as pl
import pyarrow as pa
from deltalake.writer import write_deltalake

# Database Management
import connectorx as cx
import pyodbc
import pymssql
import psycopg2
import duckdb

# Cloud Connection/APIs
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, ClientSecretCredential
from azure.devops.connection import Connection
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeFileClient
from msrest.authentication import BasicAuthentication
from azure.core.exceptions import ResourceNotFoundError

# URLs of the Gist scripts -> make_clean_names() & convert_to_text()
import requests
urls = [
    'https://gist.githubusercontent.com/renan-peres/119c19f1bdd9602e815e11855c9ea934/raw/d25fb0c41b1094c8349d697169305fb694fcb308/make-clean-names.py',
    'https://gist.githubusercontent.com/renan-peres/248b2295e33ee51f62aaaeee1fffc6ba/raw/9171361f92c2babe048ab1285a7a8b84f838e219/convert-to-text.py'
]
for url in urls:
    response = requests.get(url)
    if response.status_code == 200:
        # Execute the script content
        exec(response.text)
    else:
        print(f"Failed to fetch the script from {url}")

# ENVIRONMENT VARIABLES
ACCOUNT_NAME, WORKSPACE_ID, LAKEHOUSE_ID, ORGANIZATION_URL, PERSONAL_ACCESS_TOKEN, PROJECT_NAME, REPO_NAME = (
    os.getenv(param) for param in [
        "ACCOUNT_NAME", "WORKSPACE_ID", "LAKEHOUSE_ID", "ORGANIZATIONAL_URL", "PERSONAL_ACCESS_TOKEN", "PROJECT_NAME", "REPO_NAME"
    ]
)

# AUTHENTICATION
def get_authentication_token() -> DefaultAzureCredential:
    return DefaultAzureCredential()

def get_file_system_client(token_credential: DefaultAzureCredential) -> FileSystemClient:
    return DataLakeServiceClient(f"https://{ACCOUNT_NAME}.dfs.fabric.microsoft.com", credential=token_credential).get_file_system_client(WORKSPACE_ID)

def get_azure_repo_connection() -> Connection:
    return Connection(base_url=ORGANIZATION_URL, creds=BasicAuthentication('', PERSONAL_ACCESS_TOKEN))

def get_bearer_token() -> str:
    return InteractiveBrowserCredential().get_token("https://api.fabric.microsoft.com/.default").token

# UPLOAD OPERATIONS
def upload_local_file(file_client: DataLakeFileClient, source: str) -> None:
    try:
        with open(source, "rb") as file:
            file_client.upload_data(file.read(), overwrite=True)
        print(f"[I] Successfully uploaded '{source}'")
    except Exception as e:
        print(f"[E] Failed to upload '{source}': {e}")

def upload_local_folder(file_system_client: FileSystemClient, source: str, target: str) -> None:
    try:
        for root, _, files in os.walk(source):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, source).replace('\\', '/')
                file_client = file_system_client.get_file_client(f"{LAKEHOUSE_ID}/{os.path.join(target, relative_path)}")
                upload_local_file(file_client, local_path)
        print(f"[I] Successfully uploaded folder '{source}' to '{target}'")
    except Exception as e:
        print(f"[E] Failed to upload folder '{source}': {e}")

def upload_git_file(file_client: DataLakeFileClient, source: str, connection: Connection) -> None:
    file_content = "".join(chunk.decode('utf-8') for chunk in read_file_from_repo(connection, source))
    file_client.upload_data(file_content, overwrite=True)

def write_file_to_lakehouse(file_system_client: FileSystemClient, source_file_path: str, target_file_path: str, upload_from: str, connection: Union[Connection, None] = None) -> None:
    if upload_from == "local":
        if os.path.isdir(source_file_path):
            print(f"Uploading local folder '{source_file_path}' to '{target_file_path}'")
            upload_local_folder(file_system_client, source_file_path, target_file_path)
        elif os.path.isfile(source_file_path):
            data_path = f"{LAKEHOUSE_ID}/{target_file_path}"
            file_client = file_system_client.get_file_client(data_path)
            print(f"[I] Uploading local file '{source_file_path}' to '{target_file_path}'")
            upload_local_file(file_client, source_file_path)
        else:
            print(f"[E] Invalid source path: '{source_file_path}'")
    elif upload_from == "git" and connection:
        data_path = f"{LAKEHOUSE_ID}/{target_file_path}"
        file_client = file_system_client.get_file_client(data_path)
        print(f"[I] Uploading from git '{source_file_path}' to '{target_file_path}'")
        upload_git_file(file_client, source_file_path, connection)
    else:
        print(f"[E] Invalid upload_from value: '{upload_from}' or missing connection")

# DATABASE OPERATIONS
def process_tables(database_system: str, 
                   database_connection: Union[pyodbc.Connection, pymssql.Connection], 
                   lakehouse_path: str, 
                   database_name: Optional[str], 
                   table_type: Optional[str], 
                   table_name: Optional[Union[str, List[str]]], 
                   fetch_and_upload_table,
                   get_database_connection):
    queries = {
        "sqlserver": """
        USE [master];

            DROP TABLE IF EXISTS #TableSizes;

            CREATE TABLE #TableSizes
            (
                recid int IDENTITY (1, 1),
                DatabaseName sysname,
                SchemaName varchar(128),
                TableName varchar(128),
                NumRows bigint,
                Total_MB decimal(15, 2),
                Used_MB decimal(15, 2),
                Unused_MB decimal(15, 2)
            )

            EXEC sp_MSforeachdb 'USE [?];
            INSERT INTO #TableSizes (DatabaseName, TableName, SchemaName, NumRows, Total_MB, Used_MB, 
            Unused_MB)
            SELECT
            ''?'' as DatabaseName,
            s.Name AS SchemaName,
            t.NAME AS TableName,
            p.rows AS NumRows,
            CAST(ROUND((SUM(a.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Total_MB,
            CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Used_MB,
            CAST(ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.00, 2) AS NUMERIC(36, 2)) AS 
            Unused_MB
            FROM
            sys.tables t
            JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
            JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE
            t.name NOT LIKE ''dt%''
            AND t.is_ms_shipped = 0
            AND i.object_id > 255
            GROUP BY
            t.Name, s.Name, p.Rows
            ORDER BY
            Total_MB, t.Name';

            DROP TABLE IF EXISTS #Results;

            CREATE TABLE #Results 
            (
                TABLE_CATALOG NVARCHAR(MAX),
                TABLE_SCHEMA NVARCHAR(MAX),
                TABLE_NAME NVARCHAR(MAX),
                COLUMN_NAME NVARCHAR(MAX),
                DATA_TYPE NVARCHAR(MAX),
                TABLE_TYPE NVARCHAR(MAX)
            );

            DECLARE @dbName NVARCHAR(255);

            DECLARE dbCursor CURSOR FOR
            SELECT
                name
            FROM
                sys.databases
            WHERE
                name NOT IN ('master', 'tempdb', 'model', 'msdb');

            OPEN dbCursor;

            FETCH NEXT
            FROM
            dbCursor
            INTO
                @dbName;

            WHILE @@FETCH_STATUS = 0
            BEGIN
            DECLARE @sql NVARCHAR(MAX);

            SET
            @sql = '
            USE [' + @dbName + '];
            INSERT INTO #Results (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
            TABLE_TYPE)
            SELECT c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, 
            c.DATA_TYPE, trim(replace(LOWER(t.TABLE_TYPE), ''base '', '''')) AS TABLE_TYPE
            FROM information_schema.columns c
            JOIN information_schema.tables t ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = 
            t.TABLE_NAME
            WHERE c.TABLE_NAME NOT LIKE ''%DWBuildVersion%'' AND c.TABLE_NAME != ''sysdiagrams''
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
            ';

            EXEC sp_executesql @sql;

            FETCH NEXT
            FROM
            dbCursor
            INTO
                @dbName;
            END;

            CLOSE dbCursor;

            DEALLOCATE dbCursor;

            DROP TABLE IF EXISTS #schemaInformation;

            CREATE TABLE #schemaInformation
            (
                TABLE_CATALOG NVARCHAR(MAX),
                TABLE_SCHEMA NVARCHAR(MAX),
                TABLE_NAME NVARCHAR(MAX),
                COLUMN_NAME NVARCHAR(MAX),
                DATA_TYPE NVARCHAR(MAX),
                TABLE_TYPE NVARCHAR(MAX),
                NUM_ROWS bigint,
                TOT_MB decimal(15, 2),
                USED_MB decimal(15, 2),
                UNUSED_MB decimal(15, 2)
            );

            INSERT
                INTO
                #schemaInformation
            SELECT
                s.DatabaseName AS TABLE_CATALOG,
                s.TableName AS TABLE_SCHEMA,
                s.SchemaName AS TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.TABLE_TYPE,
                s.NumRows AS NUM_ROWS,
                s.Total_MB AS TOT_MB,
                s.Used_MB AS USED_MB,
                s.Unused_MB AS UNUSED_MB
            FROM
                #TableSizes s
            LEFT JOIN #Results c ON
                c.TABLE_NAME = s.SchemaName
            WHERE
                TABLE_TYPE IS NOT NULL
                AND s.NumRows <> 0
            ORDER BY
                TABLE_CATALOG,
                TOT_MB
            ;

            SELECT DISTINCT
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                TABLE_TYPE,
                NUM_ROWS,
                TOT_MB
            FROM
                #schemaInformation
            ORDER BY
                TABLE_CATALOG,
                TOT_MB;

			DROP TABLE #schemaInformation;
			DROP TABLE #TableSizes;
			DROP TABLE #Results;
        """,
        "mysql": '''
            SELECT 
                c.TABLE_SCHEMA, 
                c.TABLE_NAME, 
                c.COLUMN_NAME, 
                trim(replace(LOWER(t.TABLE_TYPE), 'base ', '')) AS TABLE_TYPE
            FROM information_schema.columns c
            JOIN information_schema.tables t ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.TABLE_SCHEMA NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
        ''',
        "postgres": '''
            SELECT 
                c.TABLE_CATALOG,
                c.TABLE_SCHEMA, 
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                trim(replace(LOWER(t.TABLE_TYPE), 'base ', '')) AS TABLE_TYPE
            FROM 
                information_schema.columns c
            JOIN information_schema.tables t ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.TABLE_SCHEMA NOT IN ('pg_catalog', 'information_schema', 'pgagent');
        ''',
        "interbase": '''
            SELECT
                RDB$RELATION_NAME AS TABLE_NAME,
                CASE
                    WHEN RDB$VIEW_BLR IS NULL THEN 'table'
                    WHEN RDB$VIEW_BLR IS NOT NULL THEN 'view'
                END AS TABLE_TYPE
            FROM RDB$RELATIONS;
        ''',
        "duckdb": '''
            SELECT 
                c.TABLE_CATALOG AS 'TABLE_CATALOG',
                c.TABLE_SCHEMA AS 'TABLE_SCHEMA',
                c.TABLE_NAME AS 'TABLE_NAME',
                c.COLUMN_NAME AS 'COLUMN_NAME',
                c.DATA_TYPE AS 'DATA_TYPE',
                trim(replace(LOWER(t.TABLE_TYPE), 'base ', '')) AS TABLE_TYPE
            FROM information_schema.columns c
            JOIN information_schema.tables t ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.TABLE_CATALOG NOT IN ('sample_data', 'system', 'temp');
        '''
    }

    table_columns = {
        "sqlserver": ["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",  "COLUMN_NAME", "TABLE_TYPE", "NUM_ROWS", "TOT_MB"],
        "mysql": ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "TABLE_TYPE"],
        "postgres": ["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TABLE_TYPE"],
        "interbase": ["TABLE_NAME", "TABLE_TYPE"],
        "duckdb": ["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TABLE_TYPE"]
    }

    def fetchall(cursor, query: str) -> List:
        cursor.execute(query)
        return cursor.fetchall()

    def get_all_tables(database_system: str, connection_string: Union[str, dict], query: str, table_columns: Optional[List[str]] = None) -> pd.DataFrame:
        if database_system == 'sqlserver':
            import pymssql
            with pymssql.connect(
                server=connection_string['server'],
                user=connection_string['user'],
                password=connection_string['password'],
                database=connection_string['database']
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    tables = cursor.fetchall()
                    df = pd.DataFrame.from_records(tables, columns=table_columns)
        elif database_system in ['mysql', 'duckdb']:
            conn = get_database_connection(database_system, connection_string)
            df = pl.read_database(query=query, connection=conn)
            df = df.with_columns([col.cast(pl.Utf8) for col in df])
            df = df.select([pl.all().str.strip_chars()])
            df = df.to_pandas()
        elif database_system == 'postgres':
            if isinstance(connection_string, str):
                # If connection_string is a string, use the original logic
                conn = get_database_connection(database_system, connection_string)
                cursor = conn.cursor()
                tables = fetchall(cursor, query)
                cursor.close()
                df = pd.DataFrame.from_records(tables, columns=table_columns)
            elif isinstance(connection_string, dict):
                # Construct the connection string for PostgreSQL
                connection_str = (
                    f"postgresql://{connection_string['user']}:"
                    f"{urllib.parse.quote_plus(connection_string['password'])}@"
                    f"{connection_string['server']}:"
                    f"{connection_string['port']}/"
                    f"{connection_string['database']}"
                )
                # Connect using psycopg2
                with psycopg2.connect(connection_str) as conn:
                    with conn.cursor() as cursor:
                        tables = fetchall(cursor, query)
                        df = pd.DataFrame.from_records(tables, columns=table_columns)
        else:
            conn = get_database_connection(database_system, connection_string)
            cursor = conn.cursor()
            tables = fetchall(cursor, query)
            cursor.close()
            df = pd.DataFrame.from_records(tables, columns=table_columns)

        return df

    try:
        tables_df = get_all_tables(database_system, database_connection, queries[database_system], table_columns[database_system])
        
        if database_name:
            tables_df = tables_df[tables_df[table_columns[0]].str.strip() == database_name]

        if database_system.lower() == 'interbase':
            tables_df = tables_df.drop_duplicates(subset=["TABLE_NAME", "TABLE_TYPE"])
        else:
            tables_df = tables_df.drop_duplicates(subset=["TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"])
        
        tables_df = tables_df.map(lambda x: x.strip() if isinstance(x, str) else x)

        if table_name:
            if isinstance(table_name, str):
                table_name = [table_name]
            pattern = '|'.join([re.escape(name.strip()) for name in table_name])
            tables_df = tables_df[tables_df["TABLE_NAME"].str.contains(pattern, case=False, na=False)]

        if table_type:
            tables_df = tables_df[tables_df["TABLE_TYPE"].str.lower() == table_type.lower()]

        if database_system in ['mysql', 'postgres']:
            combined_tables = tables_df['TABLE_SCHEMA'] + '.' + tables_df['TABLE_NAME']
            table_types = tables_df['TABLE_TYPE']
            table_path_dict = {f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}": f"{lakehouse_path}{database_system}_{row['TABLE_SCHEMA']}_{row['TABLE_TYPE']}_{row['TABLE_NAME']}" for _, row in tables_df.iterrows()}
        elif database_system in ['sqlserver', 'duckdb']:
            combined_tables = tables_df['TABLE_CATALOG'] + '.' + tables_df['TABLE_SCHEMA'] + '.' + tables_df['TABLE_NAME']
            table_types = tables_df['TABLE_TYPE']
            table_path_dict = {f"{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}": f"{lakehouse_path}{database_system}_{row['TABLE_CATALOG']}_{row['TABLE_TYPE']}_{row['TABLE_NAME']}" for _, row in tables_df.iterrows()}
        elif database_system == 'interbase':
            combined_tables = tables_df['TABLE_NAME']
            table_types = tables_df['TABLE_TYPE']
            table_path_dict = {f"{row['TABLE_NAME']}": f"{lakehouse_path}{database_system}_{row['TABLE_TYPE']}_{row['TABLE_NAME']}" for _, row in tables_df.iterrows()}

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(fetch_and_upload_table, table, table_type, table_path_dict, database_system, database_connection)
                for table, table_type in zip(combined_tables, table_types)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"Thread generated an exception: {exc}")

    except Exception as e:
        logging.error(f"Fetching tables failed: {e}")
    finally:
        if 'conn' in locals() and isinstance(conn, (pyodbc.Connection, pymssql.Connection)):
            conn.close()

def write_database_to_fabric(
    file_system_client,
    database_system: str,
    database_connection: Union[str, dict],
    lakehouse_path: str,
    database_name: Optional[str] = None,
    table_type: Optional[str] = None,
    table_name: Optional[Union[str, List[str]]] = None,
    convert_to_text: Optional[bool] = False,
    clean_column_names: Optional[bool] = False,
    case_type: str = "lower",
    limit_rows: Optional[int] = None,
    use_uri: bool = False
):
    logging.basicConfig(level=logging.ERROR)

    """
    write_database_to_fabric(): Writes tables from Local Database (SQLServer, MySQL, PostgreSQL, InterBase, DuckDB) as a Delta table in Fabric.
        Parameters:
        - file_system_client:  (Required): Azure Data Lake Storage (ADLS) system client for handling file operations.
        - database_system:     (Required): Database system being used. Options: "sqlserver", "mysql", "postgres", "interbase", "duckdb".
        - database_connection: (Required): Database connection object. Options: connection string dictionary or DSN name for the ODBC driver.
        - lakehouse_path:      (Required): Path in the Lakehouse where Delta tables will be stored. Options: "Tables/" or "Files/".
        - database_name:       (Optional): Name of the database. If None, all databases will be considered.
        - table_name:          (Optional): Table name (str), list of table names, or tables that contain (str) to be processed. If None, all tables will be processed.
        - table_type:          (Optional): Filter the table type. Options: "table" or "view". 
        - convert_to_text:     (Optional): Control whether to convert all columns to text. Options: True or False (default is False).
        - clean_column_names:  (Optional): Control whether to clean column names. Options: True or False (default is False).
        - case_type:           (Optional): Case conversion for column names, either "lower", "upper", or "proper" (default is "lower").
        - limit_rows:          (Optional): Limit the number of rows fetched from each table. If None, all rows will be processed.
        - use_uri:             (Optional): Use URI method for connection. Options: True (uses ConnectorX) or False (uses ODBC/DSN). Default is False.
    """

    def get_database_connection(database_system: str, database_connection: Union[str, dict]):
        if isinstance(database_connection, dict):
            if database_system == 'duckdb':
                return duckdb.connect(database_connection)
            if use_uri:
                if database_system == 'mysql':
                    return f"mysql://{database_connection['user']}:{database_connection['password']}@{database_connection['server']}"
                elif database_system == 'postgres':
                    return f"postgresql://{database_connection['user']}:{urllib.parse.quote_plus(database_connection['password'])}@{database_connection['server']}:{database_connection['port']}/{database_connection['database']}"
                elif database_system == 'sqlserver':
                    return f"mssql://{database_connection['user']}:{database_connection['password']}@{database_connection['server']}/{database_connection['database']}"
                elif database_system == 'interbase':
                    return f"interbase://{database_connection['user']}:{database_connection['password']}@{database_connection['server']}/{database_connection['database']}"
            else:
                if database_system == 'sqlserver':
                    return pymssql.connect(**database_connection)
                else:
                    return pyodbc.connect(**database_connection)
        else:
            return pyodbc.connect(dsn=database_connection)

    def fetch_and_upload_table(table, table_type, table_path_dict, database_system, conn_string):
        for retry_count in range(3):
            try:
                database_connection = get_database_connection(database_system, conn_string)
                query = f"SELECT {'TOP ' + str(limit_rows) if database_system == 'sqlserver' and limit_rows else ''}* FROM {table}"
                if limit_rows and database_system != 'sqlserver':
                    query += f" {'LIMIT' if database_system in ['mysql', 'postgres', 'duckdb'] else 'ROWS'} {limit_rows}"

                if database_system == 'duckdb':
                    df = database_connection.execute(query).arrow()
                elif use_uri:
                    df = pl.read_database_uri(query=query, uri=database_connection, engine="connectorx").lazy()
                else:
                    df = pl.read_database(query=query, connection=database_connection).lazy()

                if clean_column_names or case_type != '':
                    df = make_clean_names(df, case_type=case_type)

                if convert_to_text:
                    df = df.with_columns(pl.col("*").map_batches(lambda col: col.cast(pl.Utf8)))

                table_name = table.replace('.', '_')
                target_path = table_path_dict.get(table, '')
                if not target_path:
                    logging.error(f"Target path for table {table} not found.")
                    break

                df_arrow = df.collect().to_arrow()

                if not df_arrow:
                    logging.error(f"No data to write for table {table}.")
                    break

                with tempfile.TemporaryDirectory() as temp_dir:
                    delta_table_path = os.path.join(temp_dir, table_name)
                    write_deltalake(
                        table_or_uri=delta_table_path,
                        data=df_arrow,
                        mode="overwrite",
                        engine="rust",
                        storage_options={"allow_unsafe_rename": "true"}
                    )

                    write_file_to_lakehouse(
                        file_system_client=file_system_client,
                        upload_from="local",
                        source_file_path=delta_table_path,
                        target_file_path=target_path
                    )
                break

            except Exception as e:
                logging.error(f"Failed to fetch or upload table '{table}' on attempt {retry_count+1}: {e}")
                if retry_count == 2:
                    logging.error(f"Maximum retries reached for table '{table}'. Moving to the next table.")
                else:
                    time.sleep(3)

            finally:
                if 'database_connection' in locals() and hasattr(database_connection, 'close'):
                    database_connection.close()

    process_tables(database_system, database_connection, lakehouse_path, database_name, table_type, table_name, fetch_and_upload_table, get_database_connection)