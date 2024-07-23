# Amazon-Sales-Data-Analysis-Project-in-Pyspark

![image](https://github.com/user-attachments/assets/087b5d80-0bfc-46dc-bf96-b7f05e563992)

This project demonstrates how to utilize PySpark for analyzing Amazon sales data to gain insights into sales trends, customer behavior, and product performance. The project includes data cleaning, querying, and feature engineering to highlight the capabilities of PySpark in the e-commerce domain.

## Project Overview

PySpark’s robust capability to handle large datasets makes it an essential tool for data processing and analysis across various industries. This project leverages PySpark to analyze Amazon sales data, uncovering insights into sales trends, customer behavior, and product performance.

## Sample Data:

![image](https://github.com/user-attachments/assets/d612a241-0435-4edb-8dac-620787c10cb1)


Region: The broad geographical region of the sale (e.g., “Europe”, “Sub-Saharan Africa”).

Country: The specific country within the region where the sale occurred.

Item Type: The category of the product sold (e.g., “Baby Food”, “Office Supplies”).

Sales Channel: The method of sale, either “Online” or “Offline”.

Order Priority: The priority level assigned to the order (e.g., “H”, “C”, “L”).

Order Date: The date the order was placed.

Order ID: A unique identifier for the order.

Ship Date: The date the order was shipped.

Units Sold: The quantity of the product sold.

Unit Price: The selling price per unit of the product.

Unit Cost: The cost price per unit of the product.

Total Revenue: The total revenue generated from the sale (Units Sold * Unit Price).

Total Cost: The total cost incurred for the sale (Units Sold * Unit Cost).

Total Profit: The total profit made from the sale (Total Revenue — Total Cost).

### Key Objectives

- ⚡Which region has the highest profit, and which country within that region has the highest profit?
- ⚡What is the highest number of orders placed within a 72-hour period?
- ⚡Which month has the highest average number of units sold?
- ⚡On analyzing the financial performance of placed orders. Identify the lowest and highest margin percentages from this analysis?
- ⚡On identifying key sales trends d etermine the top 3 highest-value orders for a specific item type from the dataset?
- ⚡Which type of item is most frequently sold on weekends in the Sub-Saharan Africa region?
- ⚡Compute year-over-year growth for total revenue.

## Solution: 

To solve the questions using PySpark, we need to first create a SparkSession and load the dataset into a DataFrame. Here's how we can do it:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("AmazonSalesDataAnalysis").getOrCreate()

# Load the dataset into a DataFrame
df = spark.read.csv("AmazonSalesData.csv", header=True, inferSchema=True)
```
Now that we have loaded the dataset into a data frame, we can start answering the questions. 

Which region has the highest profit, and which country within that region has the highest profit?
- To determine the region with the highest profit and the country within that region with the highest profit, first group the dataset by region and calculate the total profit for each region. Identify the region with the highest profit, then filter the dataset to include only that region and group by country to find the country with the highest profit within that region. Finally, print the results for both the highest-profit region and country.
```python
from pyspark.sql.functions import col, sum

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

#Output : 12183211.400000004, 2425317.87
```
  
What is the highest number of orders placed within a 72-hour period?
- Grouping the data into 72-hour windows and counting the number of orders in each window. It then sorts these counts in descending order.
```python
from pyspark.sql.functions import window, count, col

# Group the data by a sliding window of 72 hours based on the 'Order Date' column
# Count the number of orders within each 72-hour window
orders = df.groupBy(window(col('Order Date'), '72 hours')) \
           .agg(count('Order ID').alias('count_order_id')) \
           .orderBy('count_order_id', ascending=False)

# Print the highest number of orders placed within any 72-hour period
print(f"Highest number of orders ordered under a 72 hr period is {orders.first()[1]}")

#Output : 2
```

Which month has the highest average number of units sold?
- Grouping the data by month, calculating the average units sold for each month, and then ordering the results to find the month with the highest average
```python
from pyspark.sql.functions import month, avg, col

# Group the data by month extracted from the 'Order Date' column
# Calculate the average number of units sold for each month
most_unit_sold = df.groupBy(month('Order Date').alias('month')) \
                   .agg(avg(col('Units Sold')).alias('avg_units_sold')) \
                   .orderBy('avg_units_sold', ascending=False).first()

# Print the month with the highest average number of units sold
print(f"The month with the highest average of units sold is {most_unit_sold['month']}")

#Output : 7
```

On analyzing the financial performance of placed orders. Identify the lowest and highest margin percentages from this analysis?
- Calculate the margin percentage for each order item by unit price minus unit cost and dividing it by unit cost. It then identifies and prints the lowest and highest margin percentages among all the orders
```python
from pyspark.sql.functions import col, round

# Rename the columns 'Unit Price' to 'SP' (Selling Price) and 'Unit Cost' to 'CP' (Cost Price)
margin = df.withColumnsRenamed({'Unit Price':'SP','Unit Cost':'CP'}) \
           .select('Order ID', 'SP', 'CP')

# Calculate the margin percentage for each order item and round it to the nearest integer
margin_percentage = margin.withColumn("Percentage", round(((col('SP') - col('CP')) / col('SP')) * 100))

# Find the order item with the lowest margin percentage
lowest_margin_percentage = margin_percentage.orderBy('Percentage', ascending=True).first()[3]

# Find the order item with the highest margin percentage
highest_margin_percentage = margin_percentage.orderBy('Percentage', ascending=False).first()[3]

# Print the lowest and highest margin percentages
print(f"Order Item with the lowest margin percentage is: {int(lowest_margin_percentage)}%")
print(f"Order Item with the highest margin percentage is: {int(highest_margin_percentage)}%")

#Output : 14,67
```

On identifying key sales trends d etermine the top 3 highest-value orders for a specific item type from the dataset?
- Partitioning the data based on ‘Item Type’ and ordering by ‘Total Profit’ in descending order. It assigns a row number to each order within its partition and filters to keep only the top 3 orders, then displays these highest-value orders.
```python
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Create a window specification to partition the data by 'Item Type' and order by 'Total Profit' in descending order
window_spec = Window.partitionBy(col('Item Type')).orderBy(col('Total Profit').desc())

# Add a row number column to the DataFrame based on the window specification
highest_valuable_orders = df.withColumn("row_id", row_number().over(window_spec)) \
                            .where(col('row_id').isin(1, 2, 3)) \
                            .drop('row_id')

# Display the top 3 highest-value orders for each item type
highest_valuable_orders.display()
```

Which type of item is most frequently sold on weekends in the Sub-Saharan Africa region?
- Filter the DataFrame for the Sub-Saharan Africa region and weekends. PySpark's weekday function returns 6for Sunday and 5 for Saturday.
```python
from pyspark.sql.functions import col, weekday
# Filter the DataFrame for the Sub-Saharan Africa region and weekends
# PySpark's weekday function returns 6for Sunday and 5for Saturday
Item_most_freqntly_sold = df\
                            .where((col("Region")=="Sub-Saharan Africa") & (weekday(col("Order Date")).isin([5,6])))\
                                .groupBy(col("Item Type")).agg({"Units Sold":"sum"})\
                                    .withColumnRenamed("sum(Units Sold)","sum_units_sold")\
                                        .orderBy("sum_units_sold",ascending=False)
Item_most_freqntly_sold.first()
# Personal Care
```

Compute year-over-year growth for total revenue.
- This requires extracting the year from the order date and performing a minus operation for year revenue of current month with that of pervious month divided by current month year revenue.
```python
from pyspark.sql.functions import year, sum, lag, round, concat, lit
from pyspark.sql.window import Window

# Aggregate revenue by year
revenue_by_year = df.groupBy(year(col("Order Date")).alias("Year")) \
    .agg(sum("Total Revenue").alias("Yearly Revenue"))

# Calculate year-over-year growth
windowSpec = Window.orderBy("Year")
revenue_with_growth = revenue_by_year.withColumn(
    "YoY Growth %",
    concat(round(((col("Yearly Revenue") - lag("Yearly Revenue", 1).over(windowSpec)) / lag("Yearly Revenue", 1).over(windowSpec))*100).cast("int").cast("string"),lit(" %"))
)

revenue_with_growth.display()
```

### Connect with me:
[<img src="https://img.icons8.com/color/48/000000/linkedin.png" width="6.5%"/>](https://www.linkedin.com/in/abhishek-das-9859a31a2/) [<img src="https://img.icons8.com/color/48/000000/stackoverflow.png" width="6.5%"/>](https://stackoverflow.com/users/14162435/abhishek-das) [<img src="https://cdn.jsdelivr.net/npm/simple-icons@v3/icons/leetcode.svg" width="6.5%"/>](https://leetcode.com/u/dabhishek316/) [<img src="https://img.icons8.com/fluent/48/000000/google-plus.png" width="6.5%"/>](abhishekdas69597@gmail.com) [<img src="https://img.icons8.com/fluent/48/000000/github.png" width="6.5%" alt="Github">](https://github.com/dabhishek316)
