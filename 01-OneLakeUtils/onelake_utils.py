import os
import io
import json
import requests
import base64
import zipfile
from datetime import datetime, timezone
import pytz
from typing import Union, Optional, Generator, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import time
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
from azure.devops.connection import Connection
from azure.core.exceptions import ResourceNotFoundError
import config

class OneLakeUtils:
    def upload_file(self, file_client: DataLakeFileClient, local_path: str, relative_path: str) -> Tuple[bool, str]:
        try:
            file_size = os.path.getsize(local_path)
            chunk_size = 4 * 1024 * 1024  # 4 MB chunks

            with open(local_path, "rb") as file:
                if file_size <= chunk_size:
                    file_client.upload_data(file.read(), overwrite=True)
                else:
                    file_client.create_file()
                    for i in range(0, file_size, chunk_size):
                        chunk = file.read(chunk_size)
                        file_client.append_data(data=chunk, offset=i)
                    file_client.flush_data(file_size)

            return True, relative_path
        except Exception as error:
            return False, f"Error uploading '{relative_path}': {str(error)}"

    def upload_folder(self, file_system_client: FileSystemClient, source: str, target: str, verbose: bool = True) -> None:
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
                        file_system_client.get_file_client(f"{config.LAKEHOUSE_ID}/{os.path.join(target, relative_path)}"),
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
                data_path = f"{config.LAKEHOUSE_ID}/{target_path}"
                file_client = file_system_client.get_file_client(data_path)
                print(f"Uploading local file '{source_path}' to '{target_path}'")
                self.upload_file(file_client, source_path, os.path.basename(source_path))
            else:
                print(f"Invalid source path: '{source_path}'")
        elif upload_from == "git" and connection:
            data_path = f"{config.LAKEHOUSE_ID}/{target_path}"
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
                repo_name = config.GITHUB_REPO_NAME
            self.upload_private_github_repo(file_system_client, repo_name, target_path, folder_path)
        elif upload_from == "azure_devops":
            if not project_name or not repo_name:
                project_name = config.PROJECT_NAME
                repo_name = config.REPO_NAME
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
