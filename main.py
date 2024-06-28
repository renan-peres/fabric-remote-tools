import os
import json
import requests
import base64
from datetime import datetime
from dotenv import load_dotenv
import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
import time  # Add this import

# Load environment variables from .env file
load_dotenv()

# Custom logging formatter
class CustomFormatter(logging.Formatter):
    def format(self, record):
        return record.getMessage()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logger.addHandler(handler)

class FabricAPIOperations:
    """
    A class to handle various operations with Microsoft Fabric APIS.
    """

    def __init__(self):
        """
        Initialize the OneLakeRemoteOperations class by loading environment variables.
        """
        self.account_name = os.getenv("ACCOUNT_NAME")
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.lakehouse_id = os.getenv("LAKEHOUSE_ID")
        self.token_file = "token_store.json"
        self.scope = "https://api.fabric.microsoft.com/.default"
        self.token_refresh_buffer = timedelta(minutes=5)

    def import_notebook_to_fabric(self, token: str, upload_from: str, source_path: str,
                                default_lakehouse_id: str = None,
                                default_lakehouse_workspace_id: str = None,
                                environment_id: str = None,
                                environment_workspace_id: str = None,
                                known_lakehouses: list = None,
                                max_workers: int = 5):
        """
        Imports a notebook into Microsoft Fabric from various sources.

        Args:
            token (str): Authentication token for API access.
            upload_from (str): Source of the notebook ('local', 'lakehouse', or 'github').
            source_path (str): Path or URL of the notebook to import.
            default_lakehouse_id (str, optional): ID of the default lakehouse.
            default_lakehouse_workspace_id (str, optional): ID of the default lakehouse workspace.
            environment_id (str, optional): ID of the environment.
            environment_workspace_id (str, optional): ID of the environment workspace.
            known_lakehouses (list, optional): List of known lakehouse IDs.
            max_workers (int, optional): Maximum number of worker threads for concurrent imports.

        This function orchestrates the import of notebooks from various sources into Microsoft Fabric.
        It handles different upload sources, manages metadata, and uses multithreading for efficiency.
        """
        original_lakehouse_id = os.getenv("LAKEHOUSE_ID")
        lakehouse_id = default_lakehouse_id or original_lakehouse_id
        lakehouse_name = os.getenv("LAKEHOUSE_NAME")
        default_lakehouse_workspace_id = default_lakehouse_workspace_id or os.getenv("WORKSPACE_ID")
        workspace_id = environment_workspace_id or default_lakehouse_workspace_id
        environment_id = environment_id or "6524967a-18dc-44ae-86d1-0ec903e7ca05"

        if not workspace_id:
            raise ValueError("workspace_id is required. Please provide it or set WORKSPACE_ID in your environment variables.")

        if not lakehouse_id:
            raise ValueError("lakehouse_id is required. Please provide it or set LAKEHOUSE_ID in your environment variables.")

        print(f"Using parameters:\nworkspace_id: {workspace_id}\nlakehouse_id: {lakehouse_id}\ndefault_lakehouse_workspace_id: {default_lakehouse_workspace_id}")

        notebooks_to_import = self._get_notebooks_to_import(upload_from, source_path)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for notebook_path in notebooks_to_import:
                future = executor.submit(
                    self._process_single_notebook,
                    upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id, lakehouse_name,
                    default_lakehouse_workspace_id, environment_id, known_lakehouses, default_lakehouse_id, original_lakehouse_id
                )
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result:
                    print(f"Successfully imported: {result}")
                else:
                    print(f"Failed to import a notebook")

    def _get_notebooks_to_import(self, upload_from, source_path):
        """
        Retrieves a list of notebooks to import based on the source.

        Args:
            upload_from (str): Source of the notebook ('local', 'lakehouse', or 'github').
            source_path (str): Path or URL of the notebook(s) to import.

        Returns:
            list: A list of notebook paths to import.

        Determines which notebooks to import based on the source. For local files,
        it can handle both single files and directories.
        """
        if upload_from == "local":
            if os.path.isdir(source_path):
                return [os.path.join(source_path, f) for f in os.listdir(source_path) if f.endswith('.ipynb')]
            else:
                return [source_path]
        elif upload_from == "lakehouse":
            return [source_path]
        elif upload_from == "github":
            return [source_path]
        else:
            print(f"Unsupported upload_from value: {upload_from}")
            return []

    def _process_single_notebook(self, upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id, lakehouse_name,
                                default_lakehouse_workspace_id, environment_id, known_lakehouses, default_lakehouse_id, original_lakehouse_id):
        """
        Processes and imports a single notebook.

        Args:
            upload_from (str): Source of the notebook.
            source_path (str): Original source path or URL.
            notebook_path (str): Specific path of the notebook file.
            token (str): Authentication token.
            workspace_id (str): ID of the workspace.
            lakehouse_id (str): ID of the lakehouse.
            lakehouse_name (str): Name of the lakehouse.
            default_lakehouse_workspace_id (str): ID of the default lakehouse workspace.
            environment_id (str): ID of the environment.
            known_lakehouses (list): List of known lakehouse IDs.
            default_lakehouse_id (str): ID of the default lakehouse.
            original_lakehouse_id (str): Original ID of the lakehouse from environment variables.

        Returns:
            str: Name of the imported notebook if successful, None otherwise.

        Handles the entire process of importing a single notebook, including loading content,
        updating metadata, and creating the notebook in Fabric.
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
            
            if upload_from == "github":
                repo_parts = source_path.split('/')
                owner = repo_parts[3]
                repo_name = repo_parts[4]
                file_path = '/'.join(repo_parts[7:])
                
                github_path = f"github/{owner}/{repo_name}/{file_path}"
                notebook_name = f"{github_path}--{timestamp}"
            elif upload_from == "local":
                notebook_name = f"local/{os.path.basename(notebook_path)}--{timestamp}"
            elif upload_from == "lakehouse":
                notebook_name = f"lakehouse/{lakehouse_name}/{notebook_path}--{timestamp}"
            else:
                notebook_name = f"{upload_from}_{os.path.splitext(os.path.basename(notebook_path))[0]}--{timestamp}"

            # Use the original_lakehouse_id for downloading when upload_from is 'lakehouse'
            download_lakehouse_id = original_lakehouse_id if upload_from == 'lakehouse' else lakehouse_id
            notebook_json = self._load_notebook_content(upload_from, source_path, notebook_path, token, workspace_id, download_lakehouse_id)
            
            if not notebook_json:
                return None

            new_metadata = {
                "language_info": {"name": "python"},
                "trident": {
                    "environment": {
                        "environmentId": environment_id,
                        "workspaceId": workspace_id
                    },
                    "lakehouse": {
                        "default_lakehouse_workspace_id": default_lakehouse_workspace_id
                    }
                }
            }

            if default_lakehouse_id:
                new_metadata["trident"]["lakehouse"]["default_lakehouse"] = default_lakehouse_id
            else:
                new_metadata["trident"]["lakehouse"]["default_lakehouse"] = lakehouse_id
                new_metadata["trident"]["lakehouse"]["default_lakehouse_name"] = lakehouse_name

            if known_lakehouses:
                new_metadata["trident"]["lakehouse"]["known_lakehouses"] = [{"id": lh} for lh in known_lakehouses]

            print(f"Updated notebook metadata: {json.dumps(new_metadata, indent=2)}")

            new_notebook = {
                "nbformat": 4,
                "nbformat_minor": 5,
                "cells": notebook_json.get("cells", []),
                "metadata": new_metadata
            }

            base64_notebook_content = base64.b64encode(json.dumps(new_notebook).encode('utf-8')).decode('utf-8')
            return self._create_notebook(notebook_name, base64_notebook_content, token, workspace_id)
        except Exception as e:
            print(f"Error processing notebook {notebook_path}: {str(e)}")
            return None

    def _load_notebook_content(self, upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id):
        """
        Loads the content of a notebook from various sources.

        Args:
            upload_from (str): Source of the notebook ('local', 'lakehouse', or 'github').
            source_path (str): Path or URL of the notebook.
            notebook_path (str): Specific path of the notebook file.
            token (str): Authentication token.
            workspace_id (str): ID of the workspace.
            lakehouse_id (str): ID of the lakehouse.

        Returns:
            dict: JSON content of the notebook.

        Loads notebook content from local files, lakehouses, or GitHub repositories.
        """
        if upload_from == "local":
            return self._load_local_notebook(notebook_path)
        elif upload_from == "lakehouse":
            return self._load_lakehouse_notebook(notebook_path, token, workspace_id, lakehouse_id)
        elif upload_from == "github":
            return self._load_github_notebook(source_path)
        else:
            print("Invalid upload_from parameter. Use 'local', 'lakehouse', or 'github'.")
            return None

    def _load_local_notebook(self, source_path):
        if os.path.exists(source_path):
            with open(source_path, "r", encoding="utf-8") as file:
                return json.load(file)
        else:
            print(f"Failed to locate the local notebook file: {source_path}")
            return None

    def _load_lakehouse_notebook(self, source_path, token, workspace_id, lakehouse_id):
        token_credential = DefaultAzureCredential()
        file_system_client = DataLakeServiceClient(
            f"https://onelake.dfs.fabric.microsoft.com",
            credential=token_credential
        ).get_file_system_client(workspace_id)

        temp_file_path = self._download_from_lakehouse_temp(file_system_client, source_path, lakehouse_id)
        if temp_file_path:
            with open(temp_file_path, "r", encoding="utf-8") as file:
                notebook_json = json.load(file)
            os.unlink(temp_file_path)  # Delete the temporary file
            return notebook_json
        else:
            print("Failed to download the notebook file from lakehouse.")
            return None

    def _load_github_notebook(self, repo_url):
        try:
            return self._download_file_from_github(repo_url)
        except Exception as e:
            print(f"Failed to download the notebook file from GitHub: {str(e)}")
            return None

    def _download_file_from_github(self, repo_url: str) -> dict:
        parts = repo_url.split('/')
        owner, repo, branch = parts[3], parts[4], parts[6]
        file_path = '/'.join(parts[7:])
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
        print(f"Attempting to download from: {raw_url}")
        response = requests.get(raw_url)
        response.raise_for_status()
        return json.loads(response.text)

    def _create_notebook(self, notebook_name, base64_notebook_content, token, workspace_id):
        """
        Creates a new notebook in Microsoft Fabric.

        Args:
            notebook_name (str): Name of the new notebook.
            base64_notebook_content (str): Base64 encoded content of the notebook.
            token (str): Authentication token.
            workspace_id (str): ID of the workspace.

        Returns:
            str: Name of the created notebook if successful, None otherwise.

        Sends a request to the Fabric API to create a new notebook with the provided content and metadata.
        """
        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        payload = {
            "displayName": notebook_name,
            "type": "Notebook",
            "description": "Notebook created via API",
            "definition": {
                "format": "ipynb",
                "parts": [{"path": "artifact.content.ipynb", "payload": base64_notebook_content, "payloadType": "InlineBase64"}]
            }
        }
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        response = requests.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()

        if response.status_code == 201:
            print(f"Notebook created successfully. ID: {response.json().get('id')}")
            return notebook_name
        elif response.status_code == 202:
            location_url = response.headers.get("Location")
            poll_result = self._poll_notebook_creation(location_url, token)
            if poll_result["success"]:
                print(f"Notebook created successfully. ID: {poll_result['id']}")
                return notebook_name
        return None

    def _poll_notebook_creation(self, location_url: str, token: str, max_retries: int = 20, retry_interval: int = 5) -> dict:
        """
        Polls the status of a notebook creation operation.

        Args:
            location_url (str): URL to poll for creation status.
            token (str): Authentication token.
            max_retries (int): Maximum number of retry attempts.
            retry_interval (int): Interval between retry attempts in seconds.

        Returns:
            dict: A dictionary containing success status and details of the operation.

        Continuously checks the status of a notebook creation operation until it succeeds,
        fails, or exceeds the maximum number of retries.
        """
        headers = {"Authorization": f"Bearer {token}"}
        for attempt in range(max_retries):
            try:
                poll_response = requests.get(location_url, headers=headers)
                poll_response.raise_for_status()
                
                response_json = poll_response.json()
                status = response_json.get('status', '').lower()
                
                if status == 'succeeded':
                    print(f"Poll response: {response_json}")
                    return {"success": True, "id": response_json.get('resourceId'), "details": response_json}
                elif status in ['failed', 'canceled']:
                    print(f"Poll response: {response_json}")
                    return {"success": False, "details": response_json}
                time.sleep(retry_interval)
            except requests.RequestException as e:
                print(f"Error during polling: {e}")
                time.sleep(retry_interval)
        
        return {"success": False, "details": "Polling exceeded maximum retries"}

    def _download_from_lakehouse_temp(self, file_system_client, source_path: str, lakehouse_id: str) -> str:
        """
        Downloads a file from a lakehouse to a temporary location.

        Args:
            file_system_client: Azure Data Lake file system client.
            source_path (str): Path of the file in the lakehouse.
            lakehouse_id (str): ID of the lakehouse.

        Returns:
            str: Path to the downloaded temporary file, or None if download fails.

        Downloads a file from a specified lakehouse to a temporary local file and
        returns the path to this temporary file.
        """
        import tempfile
        
        lakehouse_path = f"{lakehouse_id}/{source_path}"
        
        try:
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".ipynb")
            temp_file_path = temp_file.name
            
            # Download the file content
            with open(temp_file_path, "wb") as file_handle:
                download = file_client.download_file()
                download.readinto(file_handle)
            
            print(f"File downloaded to temporary location: {temp_file_path}")
            return temp_file_path
        
        except Exception as e:
            print(f"Error downloading file from '{source_path}': {str(e)}")
            return None
