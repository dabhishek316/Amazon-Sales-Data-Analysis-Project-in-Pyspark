# Import necessary functions from PySpark SQL
from pyspark.sql.functions import window, count, col

# Group the data by a sliding window of 72 hours based on the 'Order Date' column
# Count the number of orders within each 72-hour window
orders = df.groupBy(window(col('Order Date'), '72 hours')) \
           .agg(count('Order ID').alias('count_order_id')) \
           .orderBy('count_order_id', ascending=False)

# Print the highest number of orders placed within any 72-hour period
print(f"Highest number of orders ordered under a 72 hr period is {orders.first()[1]}")
