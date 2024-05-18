# Binance Data Pipeline Project


## Overview
This project aims to build a robust data pipeline for acquiring, cleaning, modeling, persisting, warehousing, and consuming data. The pipeline is designed to handle data from [Binance](https://binance.com), providing a comprehensive dataset for further analysis and visualization.

## Project Structure
- `**binance_data.py`**: Script for scraping data data from Binance website.
- **`main.py`**: Script for cleaning, transforming, and modeling the  data and for running the other module or components of the project. This is done for clarity and easy connections.
- **`s3_integration.py`**: Script for compressing and uploading data to an Amazon S3 bucket in zip format.
- **`snowflake_integration.py`**: Script for loading data from S3 into  snowflake.
- **`postgres_integration.py`**: Script for reading data from Amazon s3 bucket into RDB (PostgreSQL).
-**`schedule.py`**: script for scheduling, monitoring and automation for every 8 hours of tasks
- **`README.md`**: Document explaining project goals, design choices, and trade-offs.
- **`Technical_Documentation.pdf`**: Detailed document outlining architecture, design rationale, and technical considerations.

## Technical Documentation (Summary)
## Click here to read technical documentation : ![Technical Doc](https://github.com/ABAYA12/binance-data/blob/main/Technical%20Document.pdf)


### 1. Data Acquisition
- Utilizes `data_ingestion.py` to scrape data from Binance.com.
### 2. Data Modeling
- Cleans and transforms data in `main.py`.
### 3. Data Persistence
- Compresses and uploads data to an S3 bucket using `s3_integration.py`.
### 4. Data Warehousing
- Reads data from S3 and loads it into Snowflake via `snowflake_integration.py`.
### 5. RDB storage
- Reads data from s3 bucket and stores it in PostgreSQL
### 6. Data Consumption
- Provides flexibility to read data from S3 or a traditional RDS database for visualization. open `Binance.pbix`.
## Dashboard Visualization
### [Link to Dashboad:](https://app.powerbi.com/links/291cfjgyvn?ctid=4487b52f-f118-4830-b49d-3c298cb71075&pbi_source=linkShare)

![WhatsApp Image 2024-05-17 at 17 41 24_2cf2f69e](https://github.com/ABAYA12/binance-data/assets/127341105/67bbafc4-5baa-4b5c-b5d5-8c35d75eac42)


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
- Ishmael Kabu Abayateye
- Slvester Kodzotse 
- Peter K. Eduah 
- Abigail Odonkor 

## License
This project is licensed under the [License Name] License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- [Trestle Academy Ghana](https://www.trestleacademyghana.org/)
- [Binance](https://www.binance.com/en)
