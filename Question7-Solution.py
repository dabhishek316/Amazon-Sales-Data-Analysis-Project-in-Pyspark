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
