# DataOpsHouse-Pipeline
# PySpark Data Processing

This project is about processing data using PySpark. The data is processed through three layers: Raw, Curated, and Consumption.

## Raw Layer

The raw layer is the initial stage where data is ingested into the system. At this stage, the data is stored as-is without any transformation or processing. The data is loaded into a PySpark DataFrame and some exploratory analysis is performed. This includes:

- Reading the CSV file into a DataFrame
- Showing the first few rows of the DataFrame
- Printing the schema of the DataFrame
- Getting the number of rows and columns in the DataFrame
- Getting summary statistics for numerical columns
- Checking for duplicate rows and missing values in each column

## Curated Layer

The curated layer is the stage where the data is cleaned, transformed, and enriched. Business rules are applied at this stage. Once the transformations are done, the transformed data is written to the 'curated' layer in the Data Lake. The tasks performed in this stage include:

- Converting column names to camel case
- Adding filename, ingestion date, and loading time
- Converting 'Order Date' to DateType
- Partitioning the data
- Splitting 'Customer Name' into 'customerFirstName' and 'customerLastName'
- Defining window for calculating quantity of orders
- Calculating quantity of orders for last 5, 15, and 30 days
- Calculating total quantity of orders
- Writing the data to a Parquet file

## Consumption Layer

The consumption layer is the final stage where the data is ready for use by end-users or downstream applications. The data is structured for specific use-cases. A 'sales' and a 'customers' view of the data are created. These 'views' are then written out to the 'consumption' layer in the Data Lake. The steps include:

- Creating the Sales DataFrame
- Creating the Customers DataFrame
- Writing the Sales and Customers DataFrames to the 'consumption' layer of the Data Lake as Parquet

## Running the code

To run the code, you need to have PySpark installed. You can run the code in a PySpark environment or submit it as a Spark job.

The code is logged using the Python logging module, so you can track the progress and debug any issues.

## Contact

For any questions or issues, please contact [Irfan Karadeniz and ikrdnz94@gmail.com].
