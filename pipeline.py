from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth, to_date
from pyspark.sql import Window
from pyspark.sql.functions import col, count, split, sum
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')
"""
Raw Layer:
This is the initial stage where the data is ingested into the system. 
At this stage, the data is stored as-is without any transformation or processing. 
We are loading the data into a PySpark DataFrame and performing 
some exploratory analysis.
"""
# Initialize a SparkSession
spark = SparkSession.builder.appName('data_processing').getOrCreate()
# Provide the path to the file
file_path = "raw/train.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)
logging.info('CSV file read into DataFrame')

# Show the first few rows of the DataFrame
df.show()
# Print the schema of the DataFrame
df.printSchema()
# Get the number of rows in the DataFrame
print("Number of rows: ", df.count())
# Get the number of columns in the DataFrame
print("Number of columns: ", len(df.columns))
# Get summary statistics for numerical columns
df.describe().show()
# Check for duplicate rows
duplicates = df.count() - df.dropDuplicates().count()
print("Number of duplicate rows: ", duplicates)
# Check for missing values in each column
for col in df.columns:
    missing = df.filter(df[col].isNull()).count()
    print(f"Number of missing values in {col}: ", missing)

"""
Curated Layer:
This is the stage where I clean, transform, and enrich the data. I apply 
business rules at this stage. Once the transformations are done, I write this 
transformed data to the 'curated' layer in my Data Lake.
"""

# Converting Column Names to camel case
def to_camel_case(name):
    components = name.split(' ')
    if len(components) > 1:
        return components[0].lower() + ''.join(word.capitalize() for word in components[1:])
    else:
        return name.lower()

# Define filename to add
file_path = 'salesData_curated'
logging.info('Filename defined')

# Rename columns
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))
logging.info('Columns renamed')

# Add filename, ingestion date, and loading time
df = (df.withColumn('filename', lit(file_path))
        .withColumn('ingestionDate', current_date())
        .withColumn('loadingTime', current_timestamp()))
logging.info('Metadata added')

# Convert 'Order Date' to DateType
df = df.withColumn('orderDate', to_date('orderDate', 'MM/dd/yyyy')) 
logging.info("'Order Date' converted to DateType")

# Partition the Data
df = (df.withColumn('year', year('orderDate'))
        .withColumn('month', month('orderDate'))
        .withColumn('day', dayofmonth('orderDate')))
logging.info('Data partitioned')

# Split 'Customer Name' into 'customerFirstName' and 'customerLastName'
split_col = split(df['customerName'], ' ')
df = df.withColumn('customerFirstName', split_col.getItem(0))
df = df.withColumn('customerLastName', split_col.getItem(1))
logging.info("'Customer Name' split into 'customerFirstName' and 'customerLastName'")

# Define window for calculating quantity of orders
window_5 = Window.partitionBy('customerId').orderBy('orderDate').rowsBetween(-4, Window.currentRow) # for last 5 days
window_15 = Window.partitionBy('customerId').orderBy('orderDate').rowsBetween(-14, Window.currentRow) # for last 15 days
window_30 = Window.partitionBy('customerId').orderBy('orderDate').rowsBetween(-29, Window.currentRow) # for last 30 days
logging.info('Windows defined for calculating quantity of orders')

# Calculate quantity of orders for last 5, 15, and 30 days
df = df.withColumn('quantityOfOrders(last5Days)', count('orderId').over(window_5))
df = df.withColumn('quantityOfOrders(last15Days)', count('orderId').over(window_15))
df = df.withColumn('quantityOfOrders(last30Days)', count('orderId').over(window_30))
logging.info('Calculated quantity of orders for last 5, 15, and 30 days')

# Calculate total quantity of orders
df = df.withColumn('totalQuantityOfOrders', count('orderId').over(Window.partitionBy('customerId')))
logging.info('Calculated total quantity of orders')

# Writing to a parquet file
df.write.mode('overwrite').parquet('curated/curated.parquet')
logging.info('Curated data written to parquet')


"""
Consumption Layer:
This is the final stage where the data is ready for use by end-users or downstream applications.
The data is structured for specific use-cases. I create 'sales' and a 'customers' view of the data. 
These 'views' then be written out to the 'consumption' layer in my Data Lake.
"""

# Create the Sales DataFrame
df_sales = df.select('orderId', 'orderDate', 'shipDate', 'shipMode', 'city')
logging.info('Sales DataFrame created')

# Create the Customers DataFrame
df_customers = df.select('customerId', 'customerName', 'customerFirstName', 'customerLastName', 'segment', 'country', 'city', 'quantityOfOrders(last5Days)', 'quantityOfOrders(last15Days)', 'quantityOfOrders(last30Days)', 'totalQuantityOfOrders')
logging.info('Customers DataFrame created')

# Write the Sales and Customers DataFrames to the 'consumption' layer of the Data Lake as Parquet
consumption_path_sales = 'consumption/sales.parquet'  
consumption_path_customers = 'consumption/customers.parquet' 
df_sales.write.mode('overwrite').parquet(consumption_path_sales)
df_customers.write.mode('overwrite').parquet(consumption_path_customers)
logging.info('DataFrames written to the consumption layer')