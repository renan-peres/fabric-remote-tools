# Fabric Remote Tools

This repo provides a set of Python scripts for working with Microsoft Fabric and OneLake remotely.

The project provides a comprehensive solution for managing **Tables** and **Files** from a Lakehouse in Microsoft Fabric OneLake using the [azure-identity](https://pypi.org/project/azure-identity/) and [azure-storage-file-datalake](https://pypi.org/project/azure-storage-file-datalake/) Python SDKs.

### References
- Microsoft Documentation: [Use Python to manage files and folders in Microsoft OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-python)
- GitHub: [Azure-Samples/modern-data-warehouse-dataops/single_tech_samples/fabric/fabric_ci_cd](https://github.com/Azure-Samples/modern-data-warehouse-dataops/tree/main/single_tech_samples/fabric/fabric_ci_cd)
- GitHub: [microsoft/semantic-link-labs](https://github.com/microsoft/semantic-link-labs/)
- GitHub: [djouallah/Light_ETL_Challenge](https://github.com/djouallah/Light_ETL_Challenge)
- Fivetran: [OneLake Setup Guide](https://fivetran.com/docs/destinations/onelake/setup-guide)

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Get Authentication Variables & Credentials](#get-authentication-variables--credentials)
  - [Azure Portal Steps](#azure-portal-steps)
  - [Fabric Portal Steps](#fabric-portal-steps)
  - [Azure DevOps Steps](#azure-devops-steps)
  - [GitHub Steps](#github-steps-required-for-private-repos-only)
- [Usage](#usage)
  - [Running `main.py`](#running-mainpy)
  - [Testing with `test.ipynb`](#testing-with-testipynb)
    - [First Steps](#first-steps)
    - [Write to Lakehouse (Files/Tables)](#write-to-lakehouse-filestables)
    - [List Items from Lakehouse (Files/Tables)](#list-items-from-lakehouse-filestables)
    - [Read Delta Table from Lakehouse](#read-delta-table-from-lakehouse)
    - [Download Items from Lakehouse (Files/Tables)](#download-items-from-lakehouse-filestables)
    - [Delete Items from Lakehouse (Files/Tables)](#delete-items-from-lakehouse-filestables)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Contact](#contact)

## Features

- Authentication using Azure credentials
- File and directory management in OneLake storage
- Upload, download, list, and delete operations for Files and Tables (Delta format)
- Support for local Files/Tables, GitHub repositories (public and private), and Azure DevOps repositories
- Delta table support for efficient data storage and retrieval

## Prerequisites

- Python 3.7 or later
- pip (Python package installer)
- An Azure account with access to Microsoft Fabric
- A Microsoft Fabric workspace and Lakehouse
- Git (for version control and cloning the repository)
- An IDE or text editor (e.g., Visual Studio Code, Jupyter Notebook, PyCharm)
- GitHub account (for private repository access, if applicable)
- Azure DevOps account (for Azure DevOps repository access, if applicable)

## Installation

You can install the package directly from GitHub using pip:

```
pip install https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz
```

## Usage

Here's a quick example of how to use the package:

```python
from fabric_operations import FabricAuthOperations, OneLakeFileUtils, FabricAPIOperations

# Initialize the classes
auth_ops = FabricAuthOperations()
onelake_ops = OneLakeFileUtils()
fabric_ops = FabricAPIOperations()

# Get a bearer token
token = auth_ops.get_bearer_token()

# Use the token to perform operations
fabric_ops.run_notebook_job(token=token, notebook_id="your_notebook_id")
```

For more detailed usage instructions, please refer to the documentation.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.