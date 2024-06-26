import os
import json
from datetime import datetime, timezone, timedelta
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions
import pytz

# Constants
TOKEN_FILE = "token_store.json"
SCOPE = "https://api.fabric.microsoft.com/.default"
TOKEN_REFRESH_BUFFER = timedelta(minutes=5)  # Buffer time to refresh token before it expires

def get_bearer_token() -> tuple:
    # Authenticate via interactive browser and get a new token
    credential = InteractiveBrowserCredential(cache_persistence_options=TokenCachePersistenceOptions())
    access_token = credential.get_token(SCOPE)
    token = access_token.token
    expiration_timestamp = access_token.expires_on

    # Convert the expiration timestamp to a datetime object in UTC
    expiration_date_utc = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)

    # Convert the expiration date to Eastern Time
    eastern = pytz.timezone("US/Eastern")
    expiration_date_et = expiration_date_utc.astimezone(eastern)

    # Save the new token to file with the expiration timestamp
    save_token_to_file(token, expiration_timestamp)

    return token, expiration_date_et

# Save the token to a file
def save_token_to_file(token: str, expiration_timestamp: int):
    data = {
        "token": token,  # Save the actual bearer token
        "expires_on": expiration_timestamp  # Save the expiration timestamp
    }
    with open(TOKEN_FILE, "w") as file:
        json.dump(data, file)
    print(f"Token and expiration date saved to {TOKEN_FILE}")

# Load the token from a file
def load_token_from_file() -> tuple:
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as file:
                data = json.load(file)
                expiration_timestamp = data["expires_on"]
                expiration_date_utc = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)
                if datetime.now(timezone.utc) < expiration_date_utc:
                    token = data["token"]  # Read the actual token directly
                    # Convert the expiration date to Eastern Time
                    eastern = pytz.timezone("US/Eastern")
                    expiration_date_et = expiration_date_utc.astimezone(eastern)
                    return token, expiration_date_et
        except KeyError as e:
            print(f"Missing key in token file: {e}")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error reading token file: {e}")
    return None, None

# Usage example
if __name__ == "__main__":
    # Attempt to load the token from the file first
    token, expiration_date = load_token_from_file()

    # If the token is not found or expired, fetch a new one
    if not token or datetime.now(timezone.utc) >= expiration_date - TOKEN_REFRESH_BUFFER:
        print("Fetching a new token...")
        token, expiration_date = get_bearer_token()
    else:
        print("Using cached token...")

    # print("Bearer Token:", token)
    print("Token expires on (Eastern Time):", expiration_date)