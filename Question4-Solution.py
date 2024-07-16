# Import necessary functions from PySpark SQL
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
