from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, TokenCachePersistenceOptions
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeFileClient
from azure.devops.connection import Connection 
from azure.devops.credentials import BasicAuthentication
from azure.core.exceptions import ResourceNotFoundError

import os
import io
import json
import requests
import base64
import zipfile
from datetime import datetime, timezone, timedelta
import pytz
from typing import Union, Optional, Generator, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class FabricAuth:
    def __init__(self):
        self.account_name = os.getenv("ACCOUNT_NAME")
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.lakehouse_id = os.getenv("LAKEHOUSE_ID")
        self.organization_url = os.getenv("ORGANIZATIONAL_URL")
        self.personal_access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
        self.project_name = os.getenv("PROJECT_NAME")
        self.repo_name = os.getenv("REPO_NAME")
        self.github_token = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
        self.github_username = os.getenv("GITHUB_USERNAME")
        self.token_file = "token_store.json"
        self.scope = "https://api.fabric.microsoft.com/.default"
        self.token_refresh_buffer = timedelta(minutes=5)

    def get_authentication_token(self) -> DefaultAzureCredential:
        return DefaultAzureCredential()

    def get_bearer_token(self) -> str:
        token, expiration_date = self._load_token_from_file()
        if not token or datetime.now(timezone.utc) >= expiration_date - self.token_refresh_buffer:
            print("Fetching a new token...")
            token, expiration_date = self._get_new_token()
        else:
            print("Using cached token...")
        print("Token expires on (Eastern Time):", expiration_date)
        return token

    def _get_new_token(self) -> tuple:
        credential = InteractiveBrowserCredential(cache_persistence_options=TokenCachePersistenceOptions())
        access_token = credential.get_token(self.scope)
        token = access_token.token
        expiration_timestamp = access_token.expires_on
        expiration_date_utc = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)
        eastern = pytz.timezone("US/Eastern")
        expiration_date_et = expiration_date_utc.astimezone(eastern)
        self._save_token_to_file(token, expiration_timestamp)
        return token, expiration_date_et

    def _save_token_to_file(self, token: str, expiration_timestamp: int):
        data = {
            "token": token,
            "expires_on": expiration_timestamp
        }
        with open(self.token_file, "w") as file:
            json.dump(data, file)
        print(f"Token and expiration date saved to {self.token_file}")

    def _load_token_from_file(self) -> tuple:
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r") as file:
                    data = json.load(file)
                    expiration_timestamp = data["expires_on"]
                    expiration_date_utc = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)
                    if datetime.now(timezone.utc) < expiration_date_utc:
                        token = data["token"]
                        eastern = pytz.timezone("US/Eastern")
                        expiration_date_et = expiration_date_utc.astimezone(eastern)
                        return token, expiration_date_et
            except (KeyError, json.JSONDecodeError, ValueError) as e:
                print(f"Error reading token file: {e}")
        return None, None

    def get_file_system_client(self, token_credential: DefaultAzureCredential) -> FileSystemClient:
        """
        Get the file system client for OneLake storage.

        Args:
            token_credential (DefaultAzureCredential): The Azure credential.

        Returns:
            FileSystemClient: The file system client for OneLake storage.
        """
        return DataLakeServiceClient(
            f"https://{self.account_name}.dfs.fabric.microsoft.com",
            credential=token_credential
        ).get_file_system_client(self.workspace_id)

class OneLakeUtils:
    """
    A class to handle various operations on OneLake storage, including authentication,
    file system operations, uploads, downloads, listings, and deletions.
    """

    def __init__(self):
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.lakehouse_id = os.getenv("LAKEHOUSE_ID")
        self.organization_url = os.getenv("ORGANIZATIONAL_URL")
        self.personal_access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
        self.project_name = os.getenv("PROJECT_NAME")
        self.repo_name = os.getenv("REPO_NAME")
        self.github_token = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
        self.github_username = os.getenv("GITHUB_USERNAME")

    def get_azure_repo_connection(self) -> Connection:
        """
        Get the connection to Azure DevOps repository.

        Returns:
            Connection: The connection to Azure DevOps repository.
        """
        return Connection(base_url=self.organization_url, creds=BasicAuthentication('', self.personal_access_token))

    def upload_file(self, file_client: DataLakeFileClient, local_path: str, relative_path: str) -> Tuple[bool, str]:
        """
        Upload a single file to OneLake storage.

        Args:
            file_client (DataLakeFileClient): The file client for the target file.
            local_path (str): The local path of the file to upload.
            relative_path (str): The relative path of the file in OneLake storage.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating success and a message.
        """
        try:
            file_size = os.path.getsize(local_path)
            chunk_size = 4 * 1024 * 1024  # 4 MB chunks

            with open(local_path, "rb") as file:
                if file_size <= chunk_size:
                    # For small files, upload in one go
                    file_client.upload_data(file.read(), overwrite=True)
                else:
                    # For larger files, upload in chunks
                    file_client.create_file()
                    for i in range(0, file_size, chunk_size):
                        chunk = file.read(chunk_size)
                        file_client.append_data(data=chunk, offset=i)
                    file_client.flush_data(file_size)

            return True, relative_path
        except Exception as error:
            return False, f"Error uploading '{relative_path}': {str(error)}"

    def upload_folder(self, file_system_client: FileSystemClient, source: str, target: str, verbose: bool = True) -> None:
        """
        Upload a folder to OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            source (str): The local source folder path.
            target (str): The target path in OneLake storage.
            verbose (bool, optional): Whether to print verbose output. Defaults to True.
        """
        try:
            files_to_upload = []
            for root, _, files in os.walk(source):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, source).replace('\\', '/')
                    files_to_upload.append((local_path, relative_path))

            if verbose:
                source_description = "local Delta Table" if target.startswith("Tables/") else "local folder"
                print(f"Uploading {len(files_to_upload)} files from {source_description} '{source}' to '{target}'")

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_file = {
                    executor.submit(
                        self.upload_file,
                        file_system_client.get_file_client(f"{self.lakehouse_id}/{os.path.join(target, relative_path)}"),
                        local_path,
                        relative_path
                    ): (local_path, relative_path) for local_path, relative_path in files_to_upload
                }

                uploaded_count = 0
                for future in as_completed(future_to_file):
                    local_path, relative_path = future_to_file[future]
                    try:
                        success, message = future.result()
                        if success:
                            uploaded_count += 1
                            if verbose:
                                print(f"Uploaded: {message}")
                        else:
                            print(message)
                    except Exception as exc:
                        print(f"Error uploading '{local_path}': {exc}")

            print(f"Successfully uploaded {uploaded_count} out of {len(files_to_upload)} files to '{target}'")
        except Exception as error:
            print(f"Error uploading to '{target}': {error}")

    def upload_github_repo(self, file_system_client: FileSystemClient, repo_url: str, target_path: str, folder_path: str = None) -> None:
        """
        Upload an entire GitHub repository or a specific folder to OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            repo_url (str): The URL of the GitHub repository.
            target_path (str): The target path in OneLake storage.
            folder_path (str, optional): The specific folder within the repository to upload. If None, uploads the entire repository.
        """
        try:
            parts = repo_url.rstrip('/').split('/')
            owner, repo = parts[-2], parts[-1]
            if repo.endswith('.git'):
                repo = repo[:-4]

            zip_url = f"https://github.com/{owner}/{repo}/archive/refs/heads/main.zip"
            response = requests.get(zip_url)
            response.raise_for_status()

            with io.BytesIO(response.content) as zip_buffer:
                with zipfile.ZipFile(zip_buffer) as zip_file:
                    files_to_upload = []
                    for name in zip_file.namelist():
                        if name.endswith('/'):
                            continue
                        if folder_path:
                            if name.startswith(repo + "-main/" + folder_path):
                                relative_path = name[len(repo + "-main/" + folder_path):]
                                files_to_upload.append((relative_path, zip_file.read(name)))
                        else:
                            relative_path = name[len(repo + "-main/"):]
                            files_to_upload.append((relative_path, zip_file.read(name)))

            source_description = "GitHub Delta Table" if target_path.startswith("Tables/") else f"GitHub {'folder' if folder_path else 'repository'}"
            print(f"Uploading {len(files_to_upload)} files from {source_description} to '{target_path}'")

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for relative_path, content in files_to_upload:
                    full_path = f"{self.lakehouse_id}/{target_path}/{relative_path}"
                    file_client = file_system_client.get_file_client(full_path)
                    futures.append(executor.submit(self._upload_file, file_client, content, relative_path))

                uploaded_count = 0
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        print(f"Uploaded: {result}")
                        uploaded_count += 1

            print(f"Successfully uploaded {uploaded_count} files from {source_description} to '{target_path}'")
        except requests.RequestException as e:
            print(f"Failed to download repository: {str(e)}")
        except Exception as e:
            print(f"Failed to upload {source_description}: {str(e)}")
        
    def upload_private_github_repo(self, file_system_client: FileSystemClient, repo_name: str, target_path: str, folder_path: str = None) -> None:
        """
        Upload a private GitHub repository or a specific folder to OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            repo_name (str): The name of the private GitHub repository.
            target_path (str): The target path in OneLake storage.
            folder_path (str, optional): The specific folder within the repository to upload. If None, uploads the entire repository.
        """
        try:
            api_url = f"https://api.github.com/repos/{self.github_username}/{repo_name}/zipball"
            headers = {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()

            with io.BytesIO(response.content) as zip_buffer:
                with zipfile.ZipFile(zip_buffer) as zip_file:
                    files_to_upload = []
                    for name in zip_file.namelist():
                        if name.endswith('/'):
                            continue
                        if folder_path:
                            if name.startswith(folder_path):
                                relative_path = name[len(folder_path):].lstrip('/')
                                files_to_upload.append((relative_path, zip_file.read(name)))
                        else:
                            relative_path = '/'.join(name.split('/')[1:])
                            files_to_upload.append((relative_path, zip_file.read(name)))

            source_description = "GitHub private Delta Table" if target_path.startswith("Tables/") else "GitHub private repository"
            print(f"Uploading {len(files_to_upload)} files from {source_description} to '{target_path}'")

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for relative_path, content in files_to_upload:
                    full_path = f"{self.lakehouse_id}/{target_path}/{relative_path}"
                    file_client = file_system_client.get_file_client(full_path)
                    futures.append(executor.submit(self._upload_file, file_client, content, relative_path))

                uploaded_count = 0
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        uploaded_count += 1

            print(f"Successfully uploaded {uploaded_count} files from {source_description} to '{target_path}'")
        except requests.RequestException as e:
            print(f"Failed to download repository: {str(e)}")
        except Exception as e:
            print(f"Failed to upload {source_description}: {str(e)}")

    def _upload_file(self, file_client, content, file_name):
        """
        Helper method to upload a single file.

        Args:
            file_client: The file client for the target file.
            content: The content of the file to upload.
            file_name (str): The name of the file.

        Returns:
            str: The name of the file if upload was successful, None otherwise.
        """
        try:
            file_client.upload_data(content, overwrite=True)
            return file_name
        except Exception as e:
            print(f"Error uploading {file_name}: {str(e)}")
            return None

    def upload_azure_devops_repo(self, file_system_client: FileSystemClient, project_name: str, repo_name: str, target_path: str, folder_path: str = None) -> None:
        """
        Upload an entire Azure DevOps repository or a specific folder to OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            project_name (str): The name of the Azure DevOps project.
            repo_name (str): The name of the Azure DevOps repository.
            target_path (str): The target path in OneLake storage.
            folder_path (str, optional): The specific folder within the repository to upload. If None, uploads the entire repository.
        """
        try:
            connection = self.get_azure_repo_connection()
            git_client = connection.clients.get_git_client()

            repository = git_client.get_repository(repo_name, project_name)
            if not repository:
                raise ValueError(f"Repository not found in project '{project_name}'")

            items = list(git_client.get_items(repository.id, recursion_level='full'))
            files_to_upload = [item for item in items if not item.is_folder]
            
            if folder_path:
                folder_path = '/' + folder_path.lstrip('/')
                files_to_upload = [item for item in files_to_upload if item.path.startswith(folder_path)]
                if not files_to_upload:
                    print(f"No files found in the specified folder")
                    return

            source_description = "Azure DevOps Delta Table" if target_path.startswith("Tables/") else "Azure DevOps repository"
            print(f"Uploading {len(files_to_upload)} files from {source_description} to '{target_path}'")

            def upload_file(item):
                try:
                    file_content = git_client.get_item_content(repository.id, path=item.path)
                    content_bytes = b"".join(file_content)
                    
                    relative_path = item.path.lstrip('/')
                    if folder_path:
                        relative_path = relative_path[len(folder_path.lstrip('/')):]
                    full_path = f"{self.lakehouse_id}/{target_path}/{relative_path}"

                    file_client = file_system_client.get_file_client(full_path)
                    file_client.upload_data(content_bytes, overwrite=True)
                    return True
                except Exception as file_error:
                    print(f"Failed to upload file {item.path}: {str(file_error)}")
                    return False

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_file = {executor.submit(upload_file, item): item for item in files_to_upload}
                
                uploaded_count = sum(future.result() for future in as_completed(future_to_file))

            print(f"Successfully uploaded {uploaded_count} out of {len(files_to_upload)} files from {source_description} to '{target_path}'")
        except Exception as e:
            print(f"Failed to upload {source_description}: {str(e)}")

    def read_file_from_repo(self, connection: Connection, file_path: str) -> Generator[bytes, None, None]:
        """
        Read the file content from the Azure DevOps repository.

        Args:
            connection (Connection): The Azure DevOps connection.
            file_path (str): The path of the file in the repository.

        Returns:
            Generator[bytes, None, None]: A generator yielding the file content in chunks.
        """
        git_client = connection.clients.get_git_client()
        repository = git_client.get_repository(self.repo_name, self.project_name)
        return git_client.get_item_content(repository.id, path=file_path)
        
    def write_to_lakehouse(self, file_system_client: FileSystemClient, target_path: str, upload_from: str, source_path: str = "", connection: Union[Connection, None] = None, folder_path: str = None, project_name: str = None, repo_name: str = None) -> None:
        """
        Write data to the lakehouse from various sources.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            target_path (str): The target path in OneLake storage.
            upload_from (str): The source type ('local', 'git', 'github', 'github_private', or 'azure_devops').
            source_path (str, optional): The source path for local or git uploads.
            connection (Union[Connection, None], optional): The connection for git uploads.
            folder_path (str, optional): The specific folder to upload for GitHub or Azure DevOps repositories.
            project_name (str, optional): The project name for Azure DevOps uploads.
            repo_name (str, optional): The repository name for GitHub private or Azure DevOps uploads.
        """
        if upload_from == "local":
            if os.path.isdir(source_path):
                is_multiple_tables = target_path == "Tables/" and any(os.path.isdir(os.path.join(source_path, d)) for d in os.listdir(source_path))
                if is_multiple_tables:
                    print(f"Uploading multiple tables from '{source_path}' to '{target_path}'")
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        futures = []
                        for table_name in os.listdir(source_path):
                            table_path = os.path.join(source_path, table_name)
                            if os.path.isdir(table_path):
                                print(f"Processing table: {table_name}")
                                futures.append(executor.submit(self.upload_folder, file_system_client, table_path, f"{target_path}{table_name}", verbose=False))
                        for future in as_completed(futures):
                            future.result()
                else:
                    source_description = "local Delta Table" if target_path.startswith("Tables/") else "local folder"
                    print(f"Uploading {source_description} '{source_path}' to '{target_path}'")
                    self.upload_folder(file_system_client, source_path, target_path, verbose=False)
            elif os.path.isfile(source_path):
                data_path = f"{self.lakehouse_id}/{target_path}"
                file_client = file_system_client.get_file_client(data_path)
                print(f"Uploading local file '{source_path}' to '{target_path}'")
                self.upload_file(file_client, source_path, os.path.basename(source_path))
            else:
                print(f"Invalid source path: '{source_path}'")
        elif upload_from == "git" and connection:
            data_path = f"{self.lakehouse_id}/{target_path}"
            file_client = file_system_client.get_file_client(data_path)
            print(f"Uploading from git '{source_path}' to '{target_path}'")
            file_content = self.read_file_from_repo(connection, source_path)
            content_str = "".join([chunk.decode('utf-8') for chunk in file_content])
            file_client.upload_data(content_str, overwrite=True)
        elif upload_from == "github":
            print(f"Uploading from GitHub '{source_path}' to '{target_path}'")
            self.upload_github_repo(file_system_client, source_path, target_path, folder_path)
        elif upload_from == "github_private":
            if not repo_name:
                repo_name = os.getenv("GITHUB_REPO_NAME")
            self.upload_private_github_repo(file_system_client, repo_name, target_path, folder_path)
        elif upload_from == "azure_devops":
            if not project_name or not repo_name:
                project_name = os.getenv("PROJECT_NAME")
                repo_name = os.getenv("REPO_NAME")
            self.upload_azure_devops_repo(file_system_client, project_name, repo_name, target_path, folder_path)
        else:
            print(f"Invalid upload_from value: '{upload_from}' or missing connection")

    def download_from_lakehouse(self, file_system_client: FileSystemClient, target_file_path: str) -> str:
        """
        Download a file or folder from OneLake storage to the current directory, preserving the directory structure.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            target_file_path (str): The target file or folder path in OneLake storage.

        Returns:
            str: The local path of the downloaded file or folder.
        """
        lakehouse_path = f"{self.lakehouse_id}/{target_file_path}"
        local_base_path = os.getcwd()  # Get the current working directory
        
        # Create the local directory structure
        local_path = os.path.join(local_base_path, target_file_path)
        os.makedirs(local_path, exist_ok=True)

        try:
            paths = list(file_system_client.get_paths(path=lakehouse_path))
            
            if not paths:
                print(f"No files found in '{target_file_path}'")
                return local_path

            if len(paths) == 1 and not paths[0].is_directory:
                # Single file download
                file_name = os.path.basename(paths[0].name)
                local_file_path = os.path.join(local_path, file_name)
                file_client = file_system_client.get_file_client(paths[0].name)
                self.download_file(file_client, local_file_path)
                print(f"Downloaded file: {local_file_path}")
            else:
                # Folder download
                print(f"Downloading folder '{target_file_path}' from lakehouse")
                self.download_folder(file_system_client, lakehouse_path, local_path)

        except Exception as e:
            print(f"Error downloading '{target_file_path}': {str(e)}")

        return os.path.abspath(local_path)

    def download_from_lakehouse_temp(self, file_system_client, source_path: str, lakehouse_id: str) -> str:
        """
        Download a file from the lakehouse to a temporary location.

        Args:
            file_system_client: The file system client for the lakehouse.
            source_path (str): The path of the file in the lakehouse.
            lakehouse_id (str): The ID of the lakehouse.

        Returns:
            str: The path to the temporary file, or None if download failed.
        """
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
        
    def download_folder(self, directory_client: FileSystemClient, lakehouse_dir_path: str, local_dir_path: str) -> None:
        """
        Download a folder from OneLake storage, preserving the directory structure.

        Args:
            directory_client (FileSystemClient): The file system client for OneLake storage.
            lakehouse_dir_path (str): The source folder path in OneLake storage.
            local_dir_path (str): The local path to save the downloaded folder.
        """
        paths = list(directory_client.get_paths(path=lakehouse_dir_path, recursive=True))
        files_to_download: List[Tuple[str, str]] = []

        for path in paths:
            lakehouse_path = path.name
            relative_path = os.path.relpath(lakehouse_path, lakehouse_dir_path)
            local_path = os.path.join(local_dir_path, relative_path)
            
            if path.is_directory:
                os.makedirs(local_path, exist_ok=True)
            else:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                files_to_download.append((lakehouse_path, local_path))

        total_files = len(files_to_download)
        print(f"Found {total_files} files to download.")

        completed = 0
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_file = {executor.submit(self.download_file, directory_client.get_file_client(lakehouse_path), local_path): (lakehouse_path, local_path) for lakehouse_path, local_path in files_to_download}
            
            for future in as_completed(future_to_file):
                lakehouse_path, local_path = future_to_file[future]
                try:
                    future.result()
                    completed += 1
                    if completed % 10 == 0 or completed == total_files:
                        elapsed_time = time.time() - start_time
                        print(f"Progress: {completed}/{total_files} files downloaded in {elapsed_time:.2f} seconds")
                except Exception as exc:
                    print(f"Error downloading {lakehouse_path}: {exc}")

        # Check for any files that didn't download and retry them individually
        remaining_files = [f for f in files_to_download if not os.path.exists(f[1])]
        if remaining_files:
            print(f"Attempting to download {len(remaining_files)} remaining files...")
            for lakehouse_path, local_path in remaining_files:
                try:
                    self.download_file(directory_client.get_file_client(lakehouse_path), local_path)
                    print(f"Successfully downloaded: {lakehouse_path}")
                    completed += 1
                except Exception as exc:
                    print(f"Failed to download: {lakehouse_path}: {exc}")

        elapsed_time = time.time() - start_time
        print(f"Download completed. {completed}/{total_files} files downloaded successfully in {elapsed_time:.2f} seconds.")
        if completed < total_files:
            print(f"{total_files - completed} files failed to download.")

    def download_file(self, file_client: DataLakeFileClient, local_path: str) -> None:
        """
        Download a file from OneLake storage.

        Args:
            file_client (DataLakeFileClient): The file client for the source file.
            local_path (str): The local path to save the downloaded file.
        """
        with open(local_path, "wb") as file_handle:
            download = file_client.download_file()
            download.readinto(file_handle)

    def read_delta_from_fabric_lakehouse(self, file_system_client: FileSystemClient, target_file_path: str) -> Optional[str]:
        """
        Download a delta table from OneLake storage and return the local path.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            target_file_path (str): The target file or folder path in OneLake storage.

        Returns:
            Optional[str]: The local path of the downloaded delta table, or None if download failed.
        """
        lakehouse_path = f"{self.lakehouse_id}/{target_file_path}"
        paths = file_system_client.get_paths(path=lakehouse_path)
        if any(path.is_directory for path in paths) and target_file_path.startswith('Tables'):
            print(f"Downloading folder '{target_file_path}' from lakehouse")
            local_dir_path = os.path.join(os.path.expanduser("~"), "Downloads", target_file_path)
            self.download_folder(file_system_client, lakehouse_path, local_dir_path)
            return local_dir_path
        else:
            print(f"[E] Invalid path or not a Tables directory: '{target_file_path}'")
            return None

    def list_items(self, file_system_client: FileSystemClient, target_directory_path: str, print_output: bool = False) -> Optional[List[str]]:
        """
        List items in a directory in OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            target_directory_path (str): The target directory path in OneLake storage.
            print_output (bool, optional): Whether to print the output. Defaults to False.

        Returns:
            Optional[List[str]]: A list of item names if print_output is False, otherwise None.
        """
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

        if print_output:
            self.list_files(file_system_client, target_directory_path, is_tables=(target_directory_path == "Tables"))
            return None
        return filtered_names

    def list_files(self, file_system_client: FileSystemClient, target_file_path: str, indent: str = "", printed_directories: set = None, printed_files: set = None, first_call: bool = True, is_tables: bool = False) -> None:
        """
        List files and directories in OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            target_file_path (str): The target file or directory path in OneLake storage.
            indent (str, optional): The indentation for the output. Defaults to "".
            printed_directories (set, optional): Set of already printed directories. Defaults to None.
            printed_files (set, optional): Set of already printed files. Defaults to None.
            first_call (bool, optional): Whether this is the first call to the function. Defaults to True.
            is_tables (bool, optional): Whether the listing is for Tables. Defaults to False.
        """
        if printed_directories is None:
            printed_directories = set()
        if printed_files is None:
            printed_files = set()

        if first_call:
            print(target_file_path + '/')
            first_call = False

        try:
            lakehouse_path = f"{self.lakehouse_id}/{target_file_path}"
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

    def delete_local_path(self, path: str) -> None:
        """
        Delete a local file or directory.

        Args:
            path (str): The path of the local file or directory to delete.
        """
        try:
            if os.path.isfile(path):
                os.remove(path)
                print(f"Deleted local file: {path}")
            elif os.path.isdir(path):
                import shutil
                shutil.rmtree(path)
                print(f"Deleted local directory: {path}")
        except Exception as error:
            print(f"Error deleting local path '{path}': {error}")

    def delete_file(self, file_system_client: FileSystemClient, lakehouse_dir_path: str) -> None:
        """
        Delete a file, table, or folder from OneLake storage.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            lakehouse_dir_path (str): The path of the file, table, or folder to delete in OneLake storage.
        """
        full_path = f"{self.lakehouse_id}/{lakehouse_dir_path}"
        try:
            if lakehouse_dir_path == "Tables/":
                self.delete_all_tables(file_system_client)
            elif lakehouse_dir_path.startswith("Tables/"):
                # Single table deletion
                table_name = lakehouse_dir_path.split('/')[-1]
                self.delete_table(file_system_client, table_name)
            elif lakehouse_dir_path == "Files/":
                self.delete_all_files(file_system_client)
            else:
                # General file or directory deletion
                self.delete_path(file_system_client, full_path)
        except Exception as e:
            print(f"Error processing '{lakehouse_dir_path}': {str(e)}")

    def delete_table(self, file_system_client: FileSystemClient, table_name: str) -> None:
        """
        Delete a single table from the Tables directory.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            table_name (str): The name of the table to delete.
        """
        table_path = f"{self.lakehouse_id}/Tables/{table_name}"
        try:
            if file_system_client.get_directory_client(table_path).exists():
                file_system_client.delete_directory(table_path)
                print(f"Deleted table: Tables/{table_name}")
            else:
                # Instead of printing, we'll just pass silently
                pass
        except Exception as e:
            print(f"Error deleting table 'Tables/{table_name}': {str(e)}")

    def delete_all_tables(self, file_system_client: FileSystemClient) -> None:
        """
        Delete all tables in the Tables directory.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
        """
        tables_path = f"{self.lakehouse_id}/Tables"
        try:
            paths = list(file_system_client.get_paths(path=tables_path))
            for path in paths:
                if path.is_directory and not path.name.endswith('_delta_log'):
                    table_name = path.name.split('/')[-1]
                    self.delete_table(file_system_client, table_name)
        except Exception as e:
            print(f"Error listing tables: {str(e)}")

    def delete_all_files(self, file_system_client: FileSystemClient) -> None:
        """
        Delete all files and directories under the 'Files/' directory.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
        """
        files_path = f"{self.lakehouse_id}/Files"
        try:
            paths = list(file_system_client.get_paths(path=files_path, recursive=False))
            for path in paths:
                self.delete_path(file_system_client, path.name)
        except Exception as e:
            print(f"Error accessing 'Files/' directory: {str(e)}")

    def delete_path(self, file_system_client: FileSystemClient, full_path: str) -> None:
        """
        Delete a file or directory.

        Args:
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            full_path (str): The full path of the file or directory to delete.
        """
        try:
            if file_system_client.get_directory_client(full_path).exists():
                file_system_client.delete_directory(full_path)
                print(f"Deleted directory: {full_path.replace(self.lakehouse_id + '/', '')}")
            elif file_system_client.get_file_client(full_path).exists():
                file_system_client.get_file_client(full_path).delete_file()
                print(f"Deleted file: {full_path.replace(self.lakehouse_id + '/', '')}")
            else:
                pass
        except ResourceNotFoundError:
            pass
        except Exception as e:
            print(f"Error deleting '{full_path.replace(self.lakehouse_id + '/', '')}': {str(e)}")

class FabricAPIs:
    """
    A class to handle various operations with Microsoft Fabric APIS.
    """
    def __init__(self):
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.lakehouse_id = os.getenv("LAKEHOUSE_ID")
        self.lakehouse_name = os.getenv("LAKEHOUSE_NAME")

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
        
    def run_notebook_job(self, token: str, notebook_id: str, workspace_id: str = None, lakehouse_id: str = None, lakehouse_name: str = None) -> str:
        """
        Run a Spark notebook job.

        Args:
            token (str): The authentication token.
            notebook_id (str): The ID of the notebook to run.
            workspace_id (str, optional): The ID of the workspace. If not provided, uses the value from environment variables or None.
            lakehouse_id (str, optional): The ID of the lakehouse. If not provided, uses the value from environment variables or None.
            lakehouse_name (str, optional): The name of the lakehouse. If not provided, uses the value from environment variables or None.

        Returns:
            str: The location URL of the triggered job, or None if the job failed to trigger.
        """
        workspace_id = workspace_id or self.workspace_id or None
        lakehouse_id = lakehouse_id or self.lakehouse_id or None
        lakehouse_name = lakehouse_name or self.lakehouse_name or None

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
        """
        Trigger a data pipeline job.

        Args:
            token (str): The authentication token.
            pipeline_id (str): The ID of the pipeline to trigger.
            workspace_id (str, optional): The ID of the workspace. If not provided, uses the value from environment variables or None.

        Returns:
            str: The location URL of the triggered job, or None if the job failed to trigger.
        """
        workspace_id = workspace_id or self.workspace_id or None

        if not workspace_id:
            print("Warning: workspace_id is not provided and not set in environment variables.")

        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id or ''}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(endpoint, headers=headers)
        if response.status_code == 202:
            return response.headers.get("Location")
        else:
            print(f"Failed to trigger pipeline job. Status code: {response.status_code}, Response text: {response.text}")
            return None

    def trigger_table_maintenance_job(self, table_name: str, token: str) -> str:
        """
        Trigger a Delta table maintenance job.

        Args:
            table_name (str): The name of the table to maintain.
            token (str): The authentication token.

        Returns:
            str: The location URL of the triggered job, or None if the job failed to trigger.
        """
        endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/lakehouses/{self.lakehouse_id}/jobs/instances?jobType=TableMaintenance"
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
        """
        Trigger maintenance jobs for all tables in the lakehouse.

        Args:
            token (str): The authentication token.
            file_system_client (FileSystemClient): The file system client for OneLake storage.
            batch_size (int, optional): Number of tables to process in each batch. Defaults to 5.
            batch_delay (int, optional): Delay between batches in seconds. Defaults to 60.
        """
        # Get the filtered subdirectory names for "Tables"
        filtered_tables = self.list_items(file_system_client=file_system_client, target_directory_path="Tables")

        # Iterate over the filtered tables in batches
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
            
            # Delay between batches
            if i + batch_size < len(filtered_tables):
                print(f"Waiting for {batch_delay} seconds before triggering the next batch...")
                time.sleep(batch_delay)