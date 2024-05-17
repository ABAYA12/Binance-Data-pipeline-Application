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
## Click on the link to read full documentaion
----->>>>>: https://docs.google.com/document/d/1xKBf3_GB0UvxeIiGeG_701HfijnF0NNLF7HzvMY3mMU/

### 1. Data Acquisition
- Utilizes `data_ingestion.py` to scrape data from Binance.com.
### 2. Data Modeling
- Cleans and transforms data in `data_modeling.py`.
### 3. Data Persistence
- Compresses and uploads data to an S3 bucket using `data_persistence.py`.
### 4. Data Warehousing
- Reads data from S3 and loads it into Redshift via `data_warehousing.py`.
### 5. Data Consumption
- Provides flexibility to read data from S3 or a traditional RDS database using `data_consumption.py`.
### 6. Data Visualization 
- To visualize the project, here is a screenshot of the data ingestion process from the Binance website: [Insert image of data_visualization.jpg in actiong

## System Design
- Adopts an Append system design for continuous addition of new data.
- Ensures historical record maintenance in both S3 and Redshift.

## Technical Considerations
- Detailed technical document will cover specific libraries, configurations, error handling, security considerations, and trade-offs.
- Discusses the rationale behind choosing binance.com, web scraping, cloud-based storage, and data warehousing solutions.

## Trade-offs and Rationale
- Explores trade-offs in design decisions, benefits of Binance.com, advantages of cloud-based solutions, and considerations for data consumption.

## Getting Started
1. Clone the repository.
2. Install dependencies (`requirements.txt`).
3. Execute scripts in the specified order to run the data pipeline.
4. Refer to technical documentation for detailed setup and usage instructions.

## Dependencies
- Python 3.11
- Libraries: BeautifulSoup, Pandas, Boto3, Psycopg2, etc. (See `requirements.txt`)

## Contributors
- Justice O. Amofa
- Ishmael Abayatey
- Slvester Kodzotse 
- Peter K. Eduah 
- Abigail Odonkor 

## License
This project is licensed under the [License Name] License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Mention any acknowledgments here.
