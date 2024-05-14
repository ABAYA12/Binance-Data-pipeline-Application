# Binance Data Pipeline Project


## Overview
This project aims to build a robust data pipeline for acquiring, cleaning, modeling, persisting, warehousing, and consuming data. The pipeline is designed to handle data from [Binance](https://binance.com), providing a comprehensive dataset for further analysis and visualization.

## Project Structure
- **data_ingestion.py**: Script for scraping data data from Binance website.
- **data_modeling.py**: Script for cleaning, transforming, and modeling the  data.
- **data_persistence.py**: Script for compressing and uploading data to an Amazon S3 bucket.
- **data_warehousing.py**: Script for loading data from S3 into a Redshift data warehouse.
- **data_consumption.py**: Script for reading data from S3 or a traditional RDS database for analysis.
- **README.md**: Document explaining project goals, design choices, and trade-offs.
- **Technical_Documentation.pdf**: Detailed document outlining architecture, design rationale, and technical considerations.

## Technical Documentation (Summary)