# Sales Data Centralization Project

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Description

Welcome to my Data Engineering project! This project is designed to showcase various data handling and transformation techniques that are essential for data engineers. As a data engineer, the aim is to create a seamless data pipeline, ensuring that data is collected, processed, and stored efficiently.

### What This Project Does
This project covers various aspects of data engineering, including:
- Data extraction from different sources (databases, APIs, S3 storage)
- Data cleaning and preprocessing
- Data transformation and conversion
- Data loading into databases

The project also provides real-world examples and demonstrates how to address common data quality and formatting issues. You will find detailed code examples and explanations for each stage of the data engineering process.

### What I Learned
Throughout this project, I learned and implemented the following key data engineering concepts:
- Connecting to databases and APIs
- Data extraction using libraries like `pandas`, `tabula`, and `requests`
- Data cleaning with focus on handling missing values, formatting issues, and regular expressions
- Data transformation for consistent data formats and structures
- Uploading data to databases

## Installation

To run this project locally, you'll need to follow these steps:

1. Clone the repository to your local machine:
   ```shell
   git clone https://github.com/your-username/your-data-engineering-project.git

## Objectives

- Centralize sales data from various sources into a single, unified database.
- Create a single source of truth for all sales-related information.
- Develop a system that facilitates easy access to sales data from a centralized location.
- Extract up-to-date metrics for making informed business decisions.

## Getting Started

To get started with this project, follow these steps:
1. **Before running the project, make sure you   have the following prerequisites installed:**    

   - [Python](https://www.python.org/): This project is written in Python. You can download and install Python from the official website.

   - [AWS Account](https://aws.amazon.com/): Some data sources, such as S3 storage, may require AWS access. You'll need an AWS account and the necessary permissions to retrieve data from AWS.

   - [AWS Command Line Interface (CLI)](https://aws.amazon.com/cli/): The AWS CLI is a command-line tool to interact with AWS services. You need to install and configure it to access AWS resources. 

   - [PgAdmin4](https://www.pgadmin.org/): PgAdmin4 is used to manage PostgreSQL databases. Make sure you have PgAdmin4 installed and configured to work with PostgreSQL databases.


2. **Clone the Repository**: Clone this repository to your local machine.

   ```bash
   git clone https://github.com/asifshaj98/multinational-retail-data-centralisation.git

3. **Set up a Python environment and install the required packages:**
```python
   pip install -r requirements.txt
```

4. **Ensure you have the necessary AWS CLI and database credentials if applicable.**

5. **Run the Python file:**
```python
   python main.py
```

## Usage
This project is designed for data engineers to learn and practice data handling and engineering techniques. You can follow the code and examples provided in the codebase to understand how data can be extracted, cleaned, transformed, and loaded into databases.

You may use the codebase as a reference for similar data engineering tasks you encounter in your own projects.


## File Structure
The project structure is organized as follows:
```bash
data_engineering_project/
│
├── database_utils.py
│
├── data_extraction.py
│
├── data_cleaning.py
│
├── main.py
│
├── requirements.txt
│
├── README.md
│
├── db_creds.yaml
│
├── local_db_creds.yaml
│
└── api_creds.yaml
```
## File Functionality 

- **database_utils.py**: Contains functions for database connection, credentials, and data uploading.

- **data_extraction.py**: Includes data extraction methods for reading from databases, APIs, and S3 storage.

- **data_cleaning.py**: Provides data cleaning and transformation functions to prepare data for analysis or loading into databases.

- **main.py**: The main project file that orchestrates the entire data engineering pipeline.

- **requirements.txt**: Lists the required Python packages for the project.

- **README.md**: You're currently reading the project's README file.

- **db_creds.yaml**: Configuration file with database connection details.

- **local_db_creds.yaml**: Configuration file with local database credentials.

- **api_creds.yaml**: Configuration file with API credentials.

This structure helps to keep the project organized and maintainable. You can find detailed information about each file in the relevant sections of this README file.

