# Fabric Operations

A Python package for Microsoft Fabric API operations.

## Installation

You can install this package directly from GitHub:

```
pip install git+https://github.com/yourusername/fabric-operations.git
```

## Usage

```python
from fabric_operations import FabricAPIOperations, OneLakeRemoteOperations

# Initialize operations
fabric_ops = FabricAPIOperations()
onelake_ops = OneLakeRemoteOperations()

# Get authentication token
token = onelake_ops.get_bearer_token()

# Use operations
fabric_ops.import_notebook_to_fabric(token=token, ...)
```

For more detailed usage instructions, please refer to the documentation.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.