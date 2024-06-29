import os
import tempfile
from typing import Union, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeFileClient
from dotenv import load_dotenv

load_dotenv()

class AzureStorageOperations:
    def __init__(self):
        self.account_name = os.getenv("ACCOUNT_NAME")
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.lakehouse_id = os.getenv("LAKEHOUSE_ID")
        self.organization_url = os.getenv("ORGANIZATIONAL_URL")
        self.personal_access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
        self.project_name = os.getenv("PROJECT_NAME")
        self.repo_name = os.getenv("REPO_NAME")

    def get_authentication_token(self) -> DefaultAzureCredential:
        return DefaultAzureCredential()

    def get_file_system_client(self, token_credential: DefaultAzureCredential) -> FileSystemClient:
        return DataLakeServiceClient(
            f"https://{self.account_name}.dfs.fabric.microsoft.com",
            credential=token_credential
        ).get_file_system_client(self.workspace_id)

    def download_folder(self, file_system_client: FileSystemClient, lakehouse_dir_path: str, local_dir_path: str) -> None:
        lakehouse_path = f"{self.lakehouse_id}/{lakehouse_dir_path}"
        paths = list(file_system_client.get_paths(path=lakehouse_path))
        
        os.makedirs(local_dir_path, exist_ok=True)

        def download_file(path):
            relative_path = os.path.relpath(path.name, lakehouse_path)
            local_file_path = os.path.join(local_dir_path, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            file_client = file_system_client.get_file_client(path.name)
            with open(local_file_path, "wb") as file_handle:
                download = file_client.download_file()
                download.readinto(file_handle)

        with ThreadPoolExecutor(max_workers=32) as executor:
            list(executor.map(download_file, [path for path in paths if not path.is_directory]))

    def download_from_lakehouse(self, file_system_client: FileSystemClient, target_file_path: str, is_delta: bool = False) -> Optional[str]:
        lakehouse_path = f"{self.lakehouse_id}/{target_file_path}"
        paths = list(file_system_client.get_paths(path=lakehouse_path))
        
        if any(path.is_directory for path in paths):
            print(f"Downloading folder '{target_file_path}' from lakehouse")
            local_folder_path = os.path.join(os.path.expanduser("~"), "Downloads", target_file_path) if is_delta else target_file_path
            self.download_folder(file_system_client, target_file_path, local_folder_path)
            return local_folder_path if is_delta else None
        else:
            print(f"[I] Downloading file '{target_file_path}' from lakehouse")
            file_client = file_system_client.get_file_client(lakehouse_path)
            local_file_name = os.path.join(os.path.expanduser("~"), "Downloads", os.path.basename(target_file_path)) if is_delta else os.path.basename(target_file_path)
            with open(local_file_name, "wb") as file_handle:
                download = file_client.download_file()
                download.readinto(file_handle)
            return local_file_name if is_delta else None

    def list_items(self, file_system_client: FileSystemClient, target_directory_path: str) -> List[str]:
        filtered_names = []
        try:
            lakehouse_path = f"{self.lakehouse_id}/{target_directory_path}"
            paths = file_system_client.get_paths(path=lakehouse_path)
            for path in paths:
                name = path.name.split('/')[-1]
                if target_directory_path == "Tables" and path.is_directory and "_delta_log" not in name and "YEAR" not in name and "_temporary" not in name:
                    filtered_names.append(name)
                elif target_directory_path == "Files" and "_delta_log" not in name and "YEAR" not in name:
                    filtered_names.append(name)
        except Exception as error:
            print(f"[E] Error listing items: {error}")
        return filtered_names

    def delete_local_path(self, path: str) -> None:
        try:
            if os.path.isfile(path):
                os.remove(path)
                print(f"[I] File '{path}' deleted successfully.")
            elif os.path.isdir(path):
                import shutil
                shutil.rmtree(path)
                print(f"[I] Directory '{path}' deleted successfully.")
        except Exception as error:
            print(f"[E] Error deleting path '{path}': {error}")