from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, sum

# Create a SparkSession
spark = SparkSession.builder.appName("AmazonSalesDataAnalysis").getOrCreate()

# Load the dataset into a DataFrame
df = spark.read.csv("AmazonSalesData.csv", header=True, inferSchema=True)

# Group the data by region and calculate the total profit for each region
region_and_country_with_profit = df.groupBy(col('Region')).sum('Total Profit')

# Order the regions by total profit in descending order and get the region with the highest profit
region_with_highest_profit = region_and_country_with_profit.orderBy("sum(Total Profit)", ascending=False).first()

# Print the region with the highest profit and its total profit
print(f"{region_with_highest_profit['Region']} Region has the highest profit with a total profit of {region_with_highest_profit['sum(Total Profit)']}")

# Filter the data to include only the rows from the region with the highest profit
# Group the data by country within that region and calculate the total profit for each country
# Order the countries by total profit in descending order and get the country with the highest profit
country_with_highest_profit = df.where(col('Region') == region_with_highest_profit['Region']) \
                                .groupBy(col('Country')).sum('Total Profit') \
                                .orderBy("sum(Total Profit)", ascending=False).first()

# Print the country with the highest profit within the region with the highest profit and its total profit
print(f"{country_with_highest_profit['Country']} country, under region {region_with_highest_profit['Region']} has the highest profit with a total profit of {country_with_highest_profit['sum(Total Profit)']}")
