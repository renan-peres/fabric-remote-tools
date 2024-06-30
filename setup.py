from setuptools import setup, find_packages
import os

# Get the current directory
current_dir = os.path.abspath(os.path.dirname(__file__))

# Set the output directory for the distribution files
dist_dir = os.path.join(current_dir)

setup(
    name='fabric_remote_tools',
    version='0.1.1',
    packages=find_packages(),
    install_requires=['python-dotenv', 'typing', 'pandas', 'polars', 'pyarrow', 'deltalake', 'connectorx', 'pyodbc', 'pymssql', 'duckdb', 'requests', 'azure-identity', 'azure-devops', 'azure-storage-file-datalake'],
    dist_directory=dist_dir
)

# python setup.py sdist