from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, TokenCachePersistenceOptions
from datetime import datetime, timezone, timedelta
import json
import os
import pytz
import config

class FabricAuth:
    def __init__(self):
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

    def get_file_system_client(self, token_credential: DefaultAzureCredential):
        from azure.storage.filedatalake import DataLakeServiceClient
        return DataLakeServiceClient(
            f"https://{config.ACCOUNT_NAME}.dfs.fabric.microsoft.com",
            credential=token_credential
        ).get_file_system_client(config.WORKSPACE_ID)

    def get_azure_repo_connection(self):
        from azure.devops.connection import Connection
        from azure.devops.credentials import BasicAuthentication
        return Connection(base_url=config.ORGANIZATIONAL_URL, creds=BasicAuthentication('', config.PERSONAL_ACCESS_TOKEN))