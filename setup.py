from setuptools import setup, find_packages

setup(
    name='fabric_remote_tools',
    version='0.1.1',
    packages=find_packages(),
    install_requires=['python-dotenv', 'typing', 'pandas', 'polars', 'pyarrow', 'deltalake', 'connectorx', 'pyodbc', 'pymssql', 'duckdb', 'requests', 'azure-identity', 'azure-devops', 'azure-storage-file-datalake']
)