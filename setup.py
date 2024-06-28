from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="fabric-remote-tools",
    version="0.1.0",
    author="Renan Peres",
    author_email="contact@renanperes.com",
    description="A package for handling Microsoft Fabric remote operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/renan-peres/fabric-remote-tools",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
    install_requires=[
        "azure-identity",
        "azure-storage-file-datalake",
        "azure-devops",
        "requests",
        "python-dotenv",
        "pytz",
    ],
)