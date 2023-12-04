# Multinational Retail Data Centralisation

- [Multinational Retail Data Centralisation](#multinational-retail-data-centralisation)
  - [Business Objective:](#business-objective)
  - [Summary](#summary)
  - [High-Level Design](#high-level-design)
  - [Detailed Design](#detailed-design)
    - [Extraction, Cleaning and Consolidation](#extraction-cleaning-and-consolidation)
      - [Users Data Cleaning](#users-data-cleaning)
      - [Payment Cards Data Cleaning](#payment-cards-data-cleaning)
      - [Retail Stores Data Cleaning](#retail-stores-data-cleaning)
      - [Product Details Data Cleaning](#product-details-data-cleaning)
      - [Orders Data Cleaning](#orders-data-cleaning)
      - [Sales Transaction Data Cleaning](#sales-transaction-data-cleaning)
    - [Data Processing Workflow](#data-processing-workflow)
    - [Database Star Schema Implementation](#database-star-schema-implementation)
    - [Analytics Queries](#analytics-queries)
  - [Installation Instructions](#installation-instructions)
  - [Usage Instructions](#usage-instructions)
    - [Basic/First - ETL followed by Analytics Workflow](#basicfirst---etl-followed-by-analytics-workflow)
    - [Analytics Only](#analytics-only)
  - [Lessons Learnt](#lessons-learnt)
  - [Possible Features / Enhancements](#possible-features--enhancements)

## Business Objective:
Retail sales data for the company is spread across variety of data sources. The goal of this work is to consolidate, make it easily accessible, and being able to perform analytics. 
This will help the business to make informed policy decisions, understand strengths and weaknesses, and stay competitive.

## Summary
First, a data extraction pipeline retrieves retail data from hybrid sources including structured, unstructured and blob formats. Each source belongs to a specific category such as payment cards data or date and time of each transaction. The data may contain empty or invalid values in one or many fields. Therefore, a domain specific cleaning step is performed. Data entries are removed when most important fields contain erroneous values such as when card number does not conform to standard. When few of non-essential fields have incorrect values they are set to a not-applicable or not-a-number type values which will not affect the analysis results.

In the second stage, this clean data is loaded into the central database tables. The database schema follows star pattern where each topical table has a primary key column which links to the central table via a foreign key relationship. Every column has designated constraint which maintains the consistency.

Finally, analytical queries are created to generate business insights based on the requirements of the sales team.

## High-Level Design
Following schematic shows an overview of data sources going through a processing pipeline which generates corresponding tables in the central database.

![Overview of Data Sources, Processing and Resulting Tables](_docs/data_processing_pipeline.png)

## Detailed Design
### Extraction, Cleaning and Consolidation


| Data Category      | Source                | Table in Central DB |
|--------------------|-----------------------|---------------------|
| Users/Customers    | AWS RDS PostgreSQL    | `dim_users`         |
| Payments Cards     | PDF in S3 bucket      | `dim_card_details`  |
| Retail Stores      | REST API Endpoint     | `dim_stores_data`   |
| Products Catalogue | CSV file in S3 bucket | `dim_products`      |
| Orders Details     | AWS RDS PostgreSQL    | `orders_table`      |
| Sales Transactions | JSON resource URI     | `dim_date_times`    |

#### Users Data Cleaning
1. Remove duplicate rows
1. Convert date of birth to date/time format and set to NaN when fails.
1. Convert joining date to date/time format and set to NaN when fails.
1. When date of birth is recent than current time, set to NaN.
1. When joining date is recent than current time, set to NaN.
1. Validate email address with a regex and set blank when fails.
1. Validate phone number with a regex and set blank when fails.

#### Payment Cards Data Cleaning
1. Remove rows with null values.
1. Clean card numbers by removing non-digit characters.
1. Validate card numbers using regex and set NaN when fails.
1. Validate expiry date using regex and set NaN when fails.
1. Validate payment date using regex and set NaN when fails.

#### Retail Stores Data Cleaning
1. Invalid staff numbers set to 0.
2. Invalid addresses with length shorter than 3, rows removed.
3. Validate latitude with regex and set to NaN when fails.
3. Validate longitude with regex and set to NaN when fails.
4. Set locality to 'N/A' where empty or null.
5. Remove rows where store code is invalid.
1. Convert opening date to date/time format and set to NaN when fails.
2. Set store type to blank when empty or invalid.
3. Remove rows where country code is invalid.
4. Set continent to empty where empty or null.

#### Product Details Data Cleaning
1. Remove rows where product name is null or empty.
2. Remove rows where product price does not match the regex or null or, empty.
1. Remove rows where product weight is null or empty.
1. Convert product all unit weights to kilograms for simplicity.
   1. Find multi-pack type products such as '8 x 200g', convert them to single bulk weight.
   2. Convert ml to kg.
   3. Convert oz to kg.
   4. Convert grams to kg.
   5. Since all are in kg now, remove the unit postfix.
#### Orders Data Cleaning
1. Remove specified columns: '1', 'first_name', 'last_name'
#### Sales Transaction Data Cleaning
1. Remove rows with invalid months, valid [1-12].
1. Remove rows with invalid year, valid 4 digit.
1. Remove rows with invalid day, valid [1-31].
1. Remove rows with invalid timestamps.
1. Remove rows with invalid date UUID format.

### Data Processing Workflow
Following figure shows step-by-step flow of data and operations in the data processing pipeline.

![](_docs/data_processing_workflow.png)

### Database Star Schema Implementation
Following figure shows steps to implement the star schema with `orders_data` table as the central table around other dimension tables from section above [Data Tables](#extraction-cleaning-and-consolidation)

![](_docs/schema_update_workflow.png)

An entity relationship diagram (ERD) of the schema is shown below:

![](_docs/StarSchema-DataCentral.png)

### Analytics Queries
- All the queries according to the specifications are store in a sub-directory `/queries` at the root of the source. 
- These queries are prefixed with requirement as `T#_{keywords_with_underscore}.sql`. For example 7th requirements is in query file `T7_headcount_country.sql`
- All queries are run in sequence using `analytics.py` script.
- It exports the outcome of all queries as a report in a Markdown format.

Following figure shows an overview of the steps performing analytics.

![](_docs/analytics_workflow.png)

## Installation Instructions
We use `poetry` to manage environments. Please make sure to have it installed.
You can refer to instructions at: https://python-poetry.org/docs/#installation

1. Clone the repository and change directory to `DataCentralisationProject`
2. Run command `poetry install` to create production environment.
3. Please obtain and save database credentials required in the root of project directory.
4. The data extraction from pdf requires Java runtime environment, please make sure that you have `JAVA_HOME` set correctly and `java` command is available.

Now you are ready to proceed.

## Usage Instructions

### Basic/First - ETL followed by Analytics Workflow
1. Run command `python workflow.py` to execute data retrieval, extraction and cleaning pipeline. This would have created dimension tables and central table in the central database.
2. Run command `python schema_update.py` to setup central database star schema. This would set primary keys in dimension tables and foreign key relations for those in central table.
3. Run command `python analytics.py` to perform all analytics queries from `/queries` directory. It will export the report with outcomes from all queries as `report_metrics_analytics.md`.

### Analytics Only
- **Individual Queries**:
Please choose any query file from `/queries` directory and run the query using `SQLTools` in VSCode or `Pgadmin4` utility for PostgreSQL.

- **Add new query**:
Create a query file in the format specified in [Analytics Queries](#analytics-queries) duly incrementing the requirement number.

- **All Queries** : Use step 3 from above full workflow.

## Lessons Learnt

1. Data sanitization is essential as real world data may contain lots of errors, it may be missing part of the data or invalid. Else data consolidation will be challenging when the setting up schema and relationships. Because unclean data causes errors which are hard to resolve as significant backtracking is required. 

For example, if a reference is not found in the dimension table while setting up foreign key relation, we need to backtrack why that datapoint is absent and rework the filtering process.

2. Knowing database schema design in advanced will be extremely helpful to understand importance of fields in the raw data. This can be used to prioritized data sanitization process. 

For example, knowing which columns are primary keys for table, we can first remove the datapoints with erroneous values for it.  

3. Encapsulation of data and functionality helps to make code much more clean secure, and highly reusable.

For example, refactoring the `DatabaseConnector` class by directly initialising the database engine with credentials. This removed holding the credentials file information, the user only has to call connect method without knowing how, and the same API can be used across the entire codebase.


## Possible Features / Enhancements
1. A web interface for the entire workflow pipeline.
2. Deploy to a VPS or Cloud using SQLite as central DB.
3. Replicate the workflow using MySQL/MariaDB for central server.
4. Improve data sanitization code by reuse.