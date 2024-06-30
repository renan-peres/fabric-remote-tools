# Fabric OneLake Storage Operations

## Overview

`OneLakeStorageOperations` is a Python class designed to manage various operations on Fabric OneLake storage, including authentication, file system operations, uploads, downloads, listings, and deletions. This class leverages the [azure-identity](https://pypi.org/project/azure-identity/) and [azure-storage-file-datalake](https://pypi.org/project/azure-storage-file-datalake/) Python SDKs to ensure secure and efficient data management.

For more information, please check: [Use Python to manage files and folders in Microsoft OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-python).

## Prerequisites

### Python Packages

Ensure the following Python packages are installed:

- `azure-identity`
- `azure-devops`
- `azure-storage-file-datalake`
- `dotenv`
- `polars` (for working with Delta tables)

You can install these packages using `pip`:

```bash
pip install azure-identity azure-devops azure-storage-file-datalake python-dotenv polars
```

### Environment Variables

Create a `.env` file with the following environment variables, which contain credentials and configuration details for Azure services, Azure DevOps, and database connections.

```env
# Azure Service principal (SP) credentials
AZURE_CLIENT_ID= "<your_azure_client_id>"
AZURE_TENANT_ID= "<your_azure_tenant_id>"
AZURE_CLIENT_SECRET= "<your_azure_client_secret>"

# Microsoft Fabric workspace and lakehouse details
ACCOUNT_NAME= "onelake"
WORKSPACE_ID= "<your_fabric_workspace_id>"
LAKEHOUSE_ID= "<your_fabric_lakehouse_id>"
LAKEHOUSE_NAME = "<your_fabric_lakehouse_name>"
```

### Loading Environment Variables

Load these environment variables in your Python code using the `dotenv` package:

```python
from dotenv import load_dotenv
load_dotenv()
```

## Class Methods

### `OneLakeStorageOperations()`

Initializes the `OneLakeStorageOperations` class by loading environment variables required for operations.

```python
from onelake_operations import OneLakeStorageOperations

# Instantiate the class
azure_ops = OneLakeStorageOperations()
```

### `get_authentication_token()`

Retrieves the default Azure credential for authentication with the **AZURE_CLIENT_SECRET** saved in the `.env` file.

- **Returns**: `DefaultAzureCredential` - The default Azure credential.

```python
# Get Authentication Token
token = azure_ops.get_authentication_token()
```

### `get_bearer_token()`

Retrieves a bearer token for authentication using interactive browser credential.

- **Returns**: `str` - The bearer token.

```python
# Get bearer token
bearer_token = azure_ops.get_bearer_token()
```

### `get_file_system_client(token_credential)`

Obtains the file system client for OneLake storage using the provided Azure credential.

- **Args**:
  - `token_credential` - The Azure token obtained with `get_authentication_token()` or `get_bearer_token()`.

- **Returns**: `FileSystemClient` - The client for accessing the OneLake file system.

```python
# Get the file system client
file_system_client = azure_ops.get_file_system_client(token)
```

### `write_to_lakehouse(file_system_client, source_path, target_path, upload_from, connection=None)`

Writes a file or folder to OneLake storage from a local source or a Git repository.

- **Args**:
  - `file_system_client`: `FileSystemClient` - The file system client.
  - `source_path`: `str` - The source file or folder path.
  - `target_path`: `str` - The target path in Lakehouse (Tables or Files).
  - `upload_from`: `str` - Source type, either "local" or "git".
  - `connection`: `Connection` (optional) - Azure DevOps connection for Git operations.

```python
# Write a local folder to the lakehouse
azure_ops.write_to_lakehouse(
    file_system_client=file_system_client,
    upload_from="local",
    source_path="../Tables/dim_salesperson_gold",  # Local path
    target_path="Files/dim_salesperson_gold"  # Lakehouse target path
)
```

### `download_from_lakehouse(file_system_client, target_file_path)`

Downloads a file or folder from OneLake storage and returns the local path of the downloaded content.

- **Args**:
  - `file_system_client`: `FileSystemClient` - The file system client.
  - `target_file_path`: `str` - The target path in OneLake storage.

- **Returns**: `str` - The local path of the downloaded file or folder.

```python
# Download a file or folder to the current directory
local_path = azure_ops.download_from_lakehouse(
    file_system_client=file_system_client,
    target_file_path="Files/polars-cookbook/lineitem_iceberg"
)

print(f"Downloaded to: {local_path}")
```

### `read_delta_from_fabric_lakehouse(file_system_client, target_file_path)`

Downloads a delta table from OneLake storage and returns the local path and the file system client.

- **Args**:
  - `file_system_client`: `FileSystemClient` - The file system client.
  - `target_file_path`: `str` - The target path in OneLake storage.

- **Returns**: `Tuple[Optional[str], FileSystemClient]` - The local path of the delta table and the file system client.

```python
import polars as pl

# Read delta tables from the lakehouse
path, _ = azure_ops.read_delta_from_fabric_lakehouse(
    file_system_client=file_system_client,
    target_file_path="Tables/dim_salesperson_gold"
)

# Load the downloaded delta table using Polars
df = pl.scan_delta(path).collect()
print(df)

# Delete the downloaded files
azure_ops.delete_local_path(path)
```

### `list_items(file_system_client, target_directory_path, print_output=False)`

Lists items in a specified directory in OneLake storage and optionally prints the output.

- **Args**:
  - `file_system_client`: `FileSystemClient` - The file system client.
  - `target_directory_path`: `str` - The target directory path in OneLake storage.
  - `print_output`: `bool` (optional) - Whether to print the directory listing. Defaults to `False`.

- **Returns**: `Optional[List[str]]` - A list of item names if `print_output` is `False`, otherwise `None`.

```python
# List items in the 'Tables' directory
items = azure_ops.list_items(
    file_system_client=file_system_client,
    target_directory_path="Tables",
    print_output=True  # Set to True to print the items
)

print(items)  # If print_output is False, this will print the list of items
```

### `delete_file(file_system_client, lakehouse_dir_path)`

Deletes a file or folder from OneLake storage.

- **Args**:
  - `file_system_client`: `FileSystemClient` - The file system client.
  - `lakehouse_dir_path`: `str` - The path of the file or folder to delete.

```python
# Delete all tables in the 'Tables' directory
azure_ops.delete_file(
    file_system_client=file_system_client,
    lakehouse_dir_path="Tables/"
)

# Delete all files in the 'Files' directory
azure_ops.delete_file(
    file_system_client=file_system_client,
    lakehouse_dir_path="Files/"
)
```

## Usage Examples

Here are some common use cases demonstrated with code snippets.

### Write Local Files to Lakehouse (Files/Tables)

Upload local files or folders to OneLake storage.

```python
from onelake_operations import OneLakeStorageOperations

# Create an instance of OneLakeStorageOperations
azure_ops = OneLakeStorageOperations()

# Get Authentication Token
token = azure_ops.get_authentication_token()
file_system_client = azure_ops.get_file_system_client(token)

# Write a local folder to the lakehouse
azure_ops.write_to_lakehouse(
    file_system_client=file_system_client,
    upload_from="local",
    source_path="../Tables/dim_salesperson_gold",  # Local path
    target_path="Files/dim_salesperson_gold"  # Lakehouse target path
)
```

### List Items from Lakehouse (Files/Tables)

List the contents of a specified directory in OneLake storage.

```python
from onelake_operations import OneLakeStorageOperations

# Create an instance of OneLakeStorageOperations
azure_ops = OneLakeStorageOperations()

# Get Authentication Token
token = azure_ops.get_authentication_token()
file_system_client = azure_ops.get_file_system_client(token)

# List all items in the 'Tables' directory
items = azure_ops.list_items(
    file_system_client=file_system_client,
    target_directory_path="Tables",
    print_output=True  # Set to True to print the items
)

print(items)  # If print_output is False, this will print the list of items
```

### Read Delta Tables from Lakehouse

Download delta tables from OneLake storage to a local directory.

```python
from onelake_operations import OneLakeStorageOperations
import polars as pl

# Create an instance of OneLakeStorageOperations
azure_ops = OneLakeStorageOperations()

# Get Authentication Token
token = azure_ops.get_authentication_token()
file_system_client = azure_ops.get_file_system_client(token)

#

 Read delta tables from the lakehouse
path, _ = azure_ops.read_delta_from_fabric_lakehouse(
    file_system_client=file_system_client,
    target_file_path="Tables/dim_salesperson_gold"  # Lakehouse path
)

# Load the downloaded delta table using Polars
df = pl.scan_delta(path).collect()
print(df)

# Delete the downloaded files
azure_ops.delete_local_path(path)
```

### Download Items from Lakehouse (Files/Tables)

Download files or folders from OneLake storage to the local directory.

```python
from onelake_operations import OneLakeStorageOperations

# Create an instance of OneLakeStorageOperations
azure_ops = OneLakeStorageOperations()

# Get Authentication Token
token = azure_ops.get_authentication_token()
file_system_client = azure_ops.get_file_system_client(token)

# Download a file or folder to the current directory
local_path = azure_ops.download_from_lakehouse(
    file_system_client=file_system_client,
    target_file_path="Files/polars-cookbook/lineitem_iceberg"  # Lakehouse path
)

print(f"Downloaded to: {local_path}")
```

### Delete Items from Lakehouse (Files/Tables)

Delete specific files or folders from OneLake storage.

```python
from onelake_operations import OneLakeStorageOperations

# Create an instance of OneLakeStorageOperations
azure_ops = OneLakeStorageOperations()

# Get Authentication Token
token = azure_ops.get_authentication_token()
file_system_client = azure_ops.get_file_system_client(token)

# Delete all tables in the 'Tables' directory
azure_ops.delete_file(
    file_system_client=file_system_client,
    lakehouse_dir_path="Tables/"  # Path to delete
)

# Delete all files in the 'Files' directory
azure_ops.delete_file(
    file_system_client=file_system_client,
    lakehouse_dir_path="Files/"  # Path to delete
)
```