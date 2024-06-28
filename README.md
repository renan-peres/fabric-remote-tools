# Fabric Operations

Fabric Operations is a Python package that provides a set of tools for working with Microsoft Fabric APIs and OneLake storage.

## Installation

You can install the package directly from GitHub using pip:

```
pip install git+https://github.com/yourusername/fabric-operations.git
```

## Usage

Here's a quick example of how to use the package:

```python
from fabric_operations import FabricAuthOperations, OneLakeFileUtils, FabricAPIOperations

# Initialize the classes
auth_ops = FabricAuthOperations()
onelake_ops = OneLakeFileUtils()
fabric_ops = FabricAPIOperations()

# Get a bearer token
token = auth_ops.get_bearer_token()

# Use the token to perform operations
fabric_ops.run_notebook_job(token=token, notebook_id="your_notebook_id")
```

For more detailed usage instructions, please refer to the documentation.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.