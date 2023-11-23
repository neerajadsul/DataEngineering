# Multinational Retail Data Centralisation

## Business Objective:
Retial sales data for the company is spread across variety of data sources. The goal of this work is to consolidate, make it easily accessible, and being able to perform analytics. 
This will help the business to make informed policy decisions, understand strengths and weaknesses, and stay compititive.

## Summary
First, a data extraction pipeline retrieves retail data from hybrid sources including structured, unstructured and blob formats. Each source belongs to a specific category such as payment cards data or date and time of each transaction. The data may contain empty or invalid values in one or many fields. Therefore a domain specific cleaning step is performed. Data entries are removed when most important fields contain errorneous values such as when card number does not conform to standard. When few of non-essential fields have incorrect values they are set to a not-applicable or not-a-number type values which will not affect the analysis results.

In the second stage, this clean data is loaded into the central database tables. The database schema follows star pattern where each topical table has a primary key column which links to the central table via a foreign key relationship. Every column has designated constraint which maintains the consistency.

Finally, analytical queries are created to generate business insights based on the requirements of the sales team.

## High-Level Design


## Detailed Design
### Data Centralisation Workflow


| Data Category | Source | Table in Central DB |
|---------------|--------|---------------------|
| Users/Customers | AWS RDS PostgreSQL | `dim_users` |
| Payments Cards | PDF in S3 bucket | `dim_card_details` |
| Retail Stores | REST API Endpoint | `dim_stores_data` |
| Products Catalogue | CSV file in S3 bucket | `dim_products` |
| Orders Details | AWS RDS PostgreSQL | `orders_table` |
| Sales Transactions | JSON resouce URI | `dim_date_times` |

#### Users Data Cleaning

#### Payment Cards Data Cleaning

#### Retail Stores Data Cleaning

#### Product Details Data Cleaning

#### Orders Data Cleaning

#### Sales Transaction Data
- Extracted from: JSON resource uri
- Cleaning: 
- Uploaded to: table named `dim_date_times` in the central database.

## Installation Instructions

## Usage Instructions

## Lessons Learnt