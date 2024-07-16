# Import necessary functions from PySpark SQL
from pyspark.sql.functions import month, avg, col

# Group the data by month extracted from the 'Order Date' column
# Calculate the average number of units sold for each month
most_unit_sold = df.groupBy(month('Order Date').alias('month')) \
                   .agg(avg(col('Units Sold')).alias('avg_units_sold')) \
                   .orderBy('avg_units_sold', ascending=False).first()

# Print the month with the highest average number of units sold
print(f"The month with the highest average of units sold is {most_unit_sold['month']}")
