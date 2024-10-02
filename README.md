# Airlines-Data-Pipeline-Project-AWS

## Introduction
In this project, I have created an End-To-End Data Engineering Project on Airlines and its operations data.

## Overview
Creating ETL pipeline designed to extract data from the source S3, transform it to ensure data quality and consistency using AWS Glue, and load it into the Redshift for further analysis and reporting.

## Architecture
to be committed

## Features
#### Data Extraction: 
1. Retrieve metadata of Airlines Flight details from the source S3 using Glue Crawler.
2. Retrieve metadata of Dimension tables related to Airport Details and crew from AWS Redshift tables using Glue Crawler.

#### Data Transformation: 
1. Apply quality checks, cleaning, and standardization to ensure high-quality data.

#### Data Loading: 
1. Load transformed data into the destination Redshift fact table for analysis. And Loading Bad Data into S3 bucket for further analysis.
2. Monitoring and Logging: Monitor the ETL pipeline's performance and log any errors or anomalies for easy troubleshooting and getting alerts on the gmail.

## Technology Used - AWS
S3 (Simple Storage Service) <br />
Glue Crawler <br />
Glue Catalog <br />
Visual ETL <br />
Redshift <br />
CloudWatch <br />
EventBridge <br />
SNS (Simple Notification Service) <br />

## DataSet Used
Here's the DataSet link - https://www.kaggle.com/datasets/iamsouravbanerjee/airline-dataset

