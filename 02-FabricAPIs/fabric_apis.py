import requests
import json
import base64
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from azure.identity import DefaultAzureCredential
import config

class FabricAPIs:
    def import_notebook_to_fabric(self, token: str, upload_from: str, source_path: str,
                                  default_lakehouse_id: str = None,
                                  default_lakehouse_workspace_id: str = None,
                                  environment_id: str = None,
                                  environment_workspace_id: str = None,
                                  known_lakehouses: list = None,
                                  max_workers: int = 5):
        lakehouse_id = default_lakehouse_id or config.LAKEHOUSE_ID
        default_lakehouse_workspace_id = default_lakehouse_workspace_id or config.WORKSPACE_ID
        workspace_id = environment_workspace_id or default_lakehouse_workspace_id
        environment_id = environment_id or "6524967a-18dc-44ae-86d1-0ec903e7ca05"

        if not workspace_id or not lakehouse_id:
            raise ValueError("workspace_id and lakehouse_id are required.")

        print(f"Using parameters:\nworkspace_id: {workspace_id}\nlakehouse_id: {lakehouse_id}\ndefault_lakehouse_workspace_id: {default_lakehouse_workspace_id}")

        notebooks_to_import = self._get_notebooks_to_import(upload_from, source_path)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for notebook_path in notebooks_to_import:
                future = executor.submit(
                    self._process_single_notebook,
                    upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id,
                    default_lakehouse_workspace_id, environment_id, known_lakehouses, default_lakehouse_id
                )
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result:
                    print(f"Successfully imported: {result}")
                else:
                    print(f"Failed to import a notebook")

    def _get_notebooks_to_import(self, upload_from, source_path):
        if upload_from == "local":
            return [os.path.join(source_path, f) for f in os.listdir(source_path) if f.endswith('.ipynb')] if os.path.isdir(source_path) else [source_path]
        elif upload_from in ["lakehouse", "github"]:
            return [source_path]
        else:
            print(f"Unsupported upload_from value: {upload_from}")
            return []

    def _process_single_notebook(self, upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id,
                                 default_lakehouse_workspace_id, environment_id, known_lakehouses, default_lakehouse_id):
        try:
            notebook_name = self._generate_notebook_name(upload_from, source_path, notebook_path)
            notebook_json = self._load_notebook_content(upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id)
            
            if not notebook_json:
                return None

            new_metadata = self._generate_metadata(environment_id, workspace_id, default_lakehouse_workspace_id, lakehouse_id, known_lakehouses)
            new_notebook = self._create_new_notebook(notebook_json, new_metadata)
            base64_notebook_content = base64.b64encode(json.dumps(new_notebook).encode('utf-8')).decode('utf-8')
            return self._create_notebook(notebook_name, base64_notebook_content, token, workspace_id)
        except Exception as e:
            print(f"Error processing notebook {notebook_path}: {str(e)}")
            return None

    def _generate_notebook_name(self, upload_from, source_path, notebook_path):
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
        if upload_from == "github":
            repo_parts = source_path.split('/')
            owner, repo_name, file_path = repo_parts[3], repo_parts[4], '/'.join(repo_parts[7:])
            return f"github/{owner}/{repo_name}/{file_path}--{timestamp}"
        elif upload_from == "local":
            return f"local/{os.path.basename(notebook_path)}--{timestamp}"
        elif upload_from == "lakehouse":
            return f"lakehouse/{config.LAKEHOUSE_NAME}/{notebook_path}--{timestamp}"
        else:
            return f"{upload_from}_{os.path.splitext(os.path.basename(notebook_path))[0]}--{timestamp}"

    def _load_notebook_content(self, upload_from, source_path, notebook_path, token, workspace_id, lakehouse_id):
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
            os.unlink(temp_file_path)
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

    def _generate_metadata(self, environment_id, workspace_id, default_lakehouse_workspace_id, lakehouse_id, known_lakehouses):
        new_metadata = {
            "language_info": {"name": "python"},
            "trident": {
                "environment": {
                    "environmentId": environment_id,
                    "workspaceId": workspace_id
                },
                "lakehouse": {
                    "default_lakehouse_workspace_id": default_lakehouse_workspace_id,
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": config.LAKEHOUSE_NAME
                }
            }
        }
        if known_lakehouses:
            new_metadata["trident"]["lakehouse"]["known_lakehouses"] = [{"id": lh} for lh in known_lakehouses]
        return new_metadata

    def _create_new_notebook(self, notebook_json, new_metadata):
        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "cells": notebook_json.get("cells", []),
            "metadata": new_metadata
        }

    def _create_notebook(self, notebook_name, base64_notebook_content, token, workspace_id):
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
        headers = {"Authorization": f"Bearer {token}"}
        for attempt in range(max_retries):
            try:
                poll_response = requests.get(location_url, headers=headers)
                poll_response.raise_for_status()
                
                response_json = poll_response.json()
                status = response_json.get('status', '').lower()
                
                if status == 'succeeded':
                    return {"success": True, "id": response_json.get('resourceId'), "details": response_json}
                elif status in ['failed', 'canceled']:
                    return {"success": False, "details": response_json}
                time.sleep(retry_interval)
            except requests.RequestException as e:
                print(f"Error during polling: {e}")
                time.sleep(retry_interval)
        
        return {"success": False, "details": "Polling exceeded maximum retries"}

    def _download_from_lakehouse_temp(self, file_system_client, source_path: str, lakehouse_id: str) -> str:
        import tempfile
        
        lakehouse_path = f"{lakehouse_id}/{source_path}"
        
        try:
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".ipynb")
            temp_file_path = temp_file.name
            
            with open(temp_file_path, "wb") as file_handle:
                download = file_client.download_file()
                download.readinto(file_handle)
            
            print(f"File downloaded to temporary location: {temp_file_path}")
            return temp_file_path
        
        except Exception as e:
            print(f"Error downloading file from '{source_path}': {str(e)}")
            return None

    def run_notebook_job(self, token: str, notebook_id: str, workspace_id: str = None, lakehouse_id: str = None, lakehouse_name: str = None) -> str:
        workspace_id = workspace_id or config.WORKSPACE_ID
        lakehouse_id = lakehouse_id or config.LAKEHOUSE_ID
        lakehouse_name = lakehouse_name or config.LAKEHOUSE_NAME

        if not workspace_id:
            print("Warning: workspace_id is not provided and not set in environment variables.")

        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
        
        payload = {
            "executionData": {
                "useStarterPool": True
            }
        }

        if lakehouse_id and lakehouse_name:
            payload["executionData"]["defaultLakehouse"] = {
                "name": lakehouse_name,
                "id": lakehouse_id,
            }
        elif lakehouse_id or lakehouse_name:
            print("Warning: Both lakehouse_id and lakehouse_name must be provided to set the default lakehouse.")

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(endpoint, json=payload, headers=headers)
        if response.status_code == 202:
            return response.headers.get("Location")
        else:
            print(f"Failed to trigger notebook job. Status code: {response.status_code}, Response text: {response.text}")
            return None

    def trigger_pipeline_job(self, token: str, pipeline_id: str, workspace_id: str = None) -> str:
        workspace_id = workspace_id or config.WORKSPACE_ID

        if not workspace_id:
            print("Warning: workspace_id is not provided and not set in environment variables.")

        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(endpoint, headers=headers)
        if response.status_code == 202:
            return response.headers.get("Location")
        else:
            print(f"Failed to trigger pipeline job. Status code: {response.status_code}, Response text: {response.text}")
            return None

    def trigger_table_maintenance_job(self, table_name: str, token: str) -> str:
        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{config.WORKSPACE_ID}/lakehouses/{config.LAKEHOUSE_ID}/jobs/instances?jobType=TableMaintenance"
        payload = {
            "executionData": {
                "tableName": table_name,
                "optimizeSettings": {"vOrder": True},
                "vacuumSettings": {"retentionPeriod": "7:01:00:00"}
            }
        }
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(endpoint, json=payload, headers=headers)
        if response.status_code == 202:
            return response.headers.get("Location")
        else:
            print(f"Failed to trigger table maintenance job. Status code: {response.status_code}, Response text: {response.text}")
            return None

    def trigger_table_maintenance_for_all_tables(self, token: str, file_system_client: FileSystemClient, batch_size: int = 5, batch_delay: int = 60):
            filtered_tables = self.list_items(file_system_client=file_system_client, target_directory_path="Tables")

            for i in range(0, len(filtered_tables), batch_size):
                batch_tables = filtered_tables[i:i + batch_size]
                for table_name in batch_tables:
                    try:
                        result = self.trigger_table_maintenance_job(table_name=table_name, token=token)
                        if result is not None:
                            print(f"Table maintenance job triggered for table: {table_name}")
                        else:
                            print(f"Failed to trigger table maintenance job for table: {table_name}")
                    except Exception as e:
                        print(f"An error occurred for table {table_name}: {e}")
                
                if i + batch_size < len(filtered_tables):
                    print(f"Waiting for {batch_delay} seconds before triggering the next batch...")
                    time.sleep(batch_delay)

        def list_items(self, file_system_client: FileSystemClient, target_directory_path: str, print_output: bool = False) -> Optional[List[str]]:
            filtered_names = []
            try:
                lakehouse_path = f"{config.LAKEHOUSE_ID}/{target_directory_path}"
                paths = file_system_client.get_paths(path=lakehouse_path)
                for path in paths:
                    name = path.name.split('/')[-1]
                    if target_directory_path == "Tables" and path.is_directory and "_delta_log" not in name and "YEAR" not in name and "_temporary" not in name:
                        filtered_names.append(name)
                    elif target_directory_path == "Files" and "_delta_log" not in name and "YEAR" not in name:
                        filtered_names.append(name)
            except Exception as error:
                print(f"[E] Error listing items: {error}")

            if print_output:
                self.list_files(file_system_client, target_directory_path, is_tables=(target_directory_path == "Tables"))
                return None
            return filtered_names

        def list_files(self, file_system_client: FileSystemClient, target_file_path: str, indent: str = "", printed_directories: set = None, printed_files: set = None, first_call: bool = True, is_tables: bool = False) -> None:
            if printed_directories is None:
                printed_directories = set()
            if printed_files is None:
                printed_files = set()

            if first_call:
                print(target_file_path + '/')
                first_call = False

            try:
                lakehouse_path = f"{config.LAKEHOUSE_ID}/{target_file_path}"
                paths = file_system_client.get_paths(path=lakehouse_path)

                for path in paths:
                    name = path.name.split('/')[-1]
                    if name not in printed_directories and name not in printed_files and "_delta_log" not in name and "YEAR" not in name:
                        if path.is_directory:
                            print(f"{indent}└── {name}/")
                            printed_directories.add(name)
                            self.list_files(file_system_client, f"{target_file_path}/{name}", indent + "    ", printed_directories, printed_files, first_call=False, is_tables=is_tables)
                        elif not is_tables or target_file_path != "Tables":
                            print(f"{indent}{'    ' if is_tables else ''}└── {name}")
                            printed_files.add(name)
            except Exception as error:
                print(f"[E] Error listing files: {error}")