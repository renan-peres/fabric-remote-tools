from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fabric-apis",
    version="0.1.0",
    author="Renan Peres",
    author_email="contact@renanperes.com",
    description="A package for Microsoft Fabric API operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/renan-peres/fabric-apis",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "requests",
        "azure-identity",
        "azure-storage-file-datalake",
        "python-dotenv",
    ],
)