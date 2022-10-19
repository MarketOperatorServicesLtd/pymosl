from setuptools import setup

setup(
    name = "pymosl",
    version = "0.1.0",
    author = "Luke Austin",
    author_email = "luke.austin@mosl.co.uk",
    packages = ["pymosl"],
    license = "LICENSE.txt",
    description = "A misc utility for accessing Azure resources, Sharepoint and Graph API for various data copy operations",
    long_description = "README.md",
    install_requires = [
        "requests", "azure-core", "azure-data-tables", 
        "azure-storage-blob", "datetime", "sqlalchemy", 
        "pyodbc", "msal", "pandas", "numpy"
        ]
    )