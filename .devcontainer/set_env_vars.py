import os

secrets = {
    # Azure Service principal (SP) credentials
    'AZURE_CLIENT_ID': os.getenv('AZURE_CLIENT_ID'),
    'AZURE_TENANT_ID': os.getenv('AZURE_TENANT_ID'),
    'AZURE_CLIENT_SECRET': os.getenv('AZURE_CLIENT_SECRET'),
    
    # Microsoft Fabric workspace and lakehouse details
    'ACCOUNT_NAME': os.getenv('ACCOUNT_NAME'),
    'WORKSPACE_ID': os.getenv('WORKSPACE_ID'),
    'LAKEHOUSE_ID': os.getenv('LAKEHOUSE_ID'),
    'LAKEHOUSE_NAME': os.getenv('LAKEHOUSE_NAME'),
    
    # Azure DevOps details and personal access token (PAT)
    'PERSONAL_ACCESS_TOKEN': os.getenv('PERSONAL_ACCESS_TOKEN'),
    'ORGANIZATIONAL_URL': os.getenv('ORGANIZATIONAL_URL'),
    'PROJECT_NAME': os.getenv('PROJECT_NAME'),
    'REPO_NAME': os.getenv('REPO_NAME'),
    
    # GitHub details and personal access token (PAT)
    'GITHUB_PERSONAL_ACCESS_TOKEN': os.getenv('GITHUB_PERSONAL_ACCESS_TOKEN'),
    'GITHUB_USERNAME': os.getenv('GITHUB_USERNAME'),
    'GITHUB_REPO_NAME': os.getenv('GITHUB_REPO_NAME')
}

with open('.env', 'w') as f:
    for key, value in secrets.items():
        f.write(f"{key}={value}\n")
