# Import necessary functions from PySpark SQL
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
