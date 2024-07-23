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
