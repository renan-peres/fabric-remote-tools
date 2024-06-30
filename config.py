import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fabric-related variables
ACCOUNT_NAME = os.getenv("ACCOUNT_NAME")
WORKSPACE_ID = os.getenv("WORKSPACE_ID")
LAKEHOUSE_ID = os.getenv("LAKEHOUSE_ID")
LAKEHOUSE_NAME = os.getenv("LAKEHOUSE_NAME")

# Azure DevOps-related variables
ORGANIZATIONAL_URL = os.getenv("ORGANIZATIONAL_URL")
PERSONAL_ACCESS_TOKEN = os.getenv("PERSONAL_ACCESS_TOKEN")
PROJECT_NAME = os.getenv("PROJECT_NAME")
REPO_NAME = os.getenv("REPO_NAME")

# GitHub-related variables
GITHUB_PERSONAL_ACCESS_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME")