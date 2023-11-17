## Multinational Retail Data Centralisation

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)


### Description

This  Multinational Retail Data Centralisation tool is a robust solution designed for comprehensive data management within a large-scale retail organisation. The primary aim is to streamline the intricate process of consolidating diverse data sets, including order details, product information, user details, and store data.

#### Objectives:

- **Automated Data Handling:** The tool automates several processes such as data extraction, cleaning, validation, and centralisation. This automation enhances the efficiency of the data analysis pipeline.

- **Versatile Data Extraction:** Specialised modules facilitate the retrieval of data from varied sources like databases, PDF files, JSON files in S3 buckets, and APIs. This data is then transformed into Pandas DataFrames, optimising it for manipulation and analysis.

- **Data Quality Assurance:** A suite of data cleaning and validation methods ensures the data is in ideal condition. It addresses missing values, data type conversion, and formatting intricacies, guaranteeing accuracy, consistency, and analysis readiness.

- **Centralised Database:** The project culminates in the centralisation of data into a Postgres database. This centralised storage ensures accessibility, security, and efficiency for future analytical endeavours.

- **Metrics Extraction:** Obtaining up-to-date metrics from the centralised database empowers the business to make more data-driven decisions. This feature involves extracting relevant data using specific SQL queries.

#### Learning Outcomes:

Throughout the development of this tool, key insights were gained:

- **Star-Based Schema Optimization:** Understanding and implementing a star-based database schema proved instrumental in supplementing the efficiency of analytical queries. This structured approach to organising dimensions and fact tables facilitates smoother data analysis.

- **Column Casting and Data Type Precision:** Learning the importance of casting columns to their correct data types significantly improved the overall integrity of the database.

- **Primary Key Implementation in Dimensions:** Establishing primary keys in dimension tables ensures data uniqueness but also contributes to maintaining data integrity. This foundational step enhances data quality and supports reliable table joins.

- **Foreign Key Constraints for Referential Integrity:** Adding foreign key constraints in the orders table proved crucial for establishing relationships with dimension tables. This not only enhances data consistency but also optimises query performance.

- **SQL Expertise for Business Insights:** Utilising Common Table Expressions (CTEs), joins, functions such as COUNT and ROUND and various other clauses in SQL queries for extracting metrics. This enhances the business's understanding of its sales, facilitating more informed decision-making processes. 

 The experience garnered from implementing these features has been invaluable in advancing my expertise in data management and analysis.


## Installation

To install the MRDC application, please follow these instructions: 

1. Clone the repository to your local computer using Git:

   ```bash
   https://github.com/michaelkielt/multinational-retail-data-centralisation.git
   ```

2. Navigate to the project directory:

   ```bash
   cd multinational-retail-data-centralisation
   ```

3. Run the application by executing the Python script:

   ```bash
   python main.py
   ```

## Usage
Running main.py will initiate the extraction, cleaning and uploading of the store data to a Postgres relational database.

## File Structure
This is how I structured my files for this project:

```
multinational-retail-data-centralisation/
│
├── main.py             # Main Python script for the project
│
├── data_cleaning.py    # Python script for cleaning and preprocessing data within a DataFrame 
│
├── data_extraction.py  # Python script for extracting data and converting it to Pandas Dataframe
│
├── database_utils      # Python script for connecting to and interacting with databases
│
├── README.md           # Documentation and instructions
│
├── mrdc_business_queries.sql   # SQL file containing all the SQL queries for milestone 4 of the project.
│
└── .gitignore          # Gitignore file to specify which files to exclude from version control
```

## License
