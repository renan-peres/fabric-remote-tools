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
    'ADO_PERSONAL_ACCESS_TOKEN': os.getenv('ADO_PERSONAL_ACCESS_TOKEN'),
    'ADO_ORGANIZATIONAL_URL': os.getenv('ADO_ORGANIZATIONAL_URL'),
    'ADO_PROJECT_NAME': os.getenv('ADO_PROJECT_NAME'),
    'ADO_REPO_NAME': os.getenv('ADO_REPO_NAME'),
    
    # GitHub details and personal access token (PAT)
    'GH_PERSONAL_ACCESS_TOKEN': os.getenv('GH_PERSONAL_ACCESS_TOKEN'),
    'GH_USERNAME': os.getenv('GH_USERNAME'),
    'GH_REPO_NAME': os.getenv('GH_REPO_NAME')
}

with open('.env', 'w') as f:
    for key, value in secrets.items():
        f.write(f"{key}={value}\n")