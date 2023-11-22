# Multinational Retail Data Centralisation

## Business Objective:


## Installation Instructions

## Usage Instructions

## High-Level Design


## Detailed Design

### Centralisation Workflow
#### Users Data
- Extracted from: an AWS RDS instance,
- Cleaning:  
- Uploaded to: table named `dim_users` in the central database.

#### Payment Cards Data
- Extracted from: a pdf file in blob storage on AWS
- Cleaning: 
- Uploaded to: table named `dim_card_details` in the central database.

#### Retail Stores Data
- Extracted from: REST API endpoints
- Cleaning: 
- Uploaded to: table named `dim_stores_data` in the central database.

#### Product Details Data
- Extracted from: a CSV file in a S3 bucket
- Cleaning: 
- Uploaded to: table named `dim_products` in the central database.

#### Orders Data
- Extracted from: orders table from an AWS RDS instance
- Cleaning: 
- Uploaded to: table named `orders_table` in the central database.

#### Sales Transaction Data
- Extracted from: JSON resource uri
- Cleaning: 
- Uploaded to: table named `dim_date_times` in the central database.

## Lessons Learnt