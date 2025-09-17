# Import your libraries
from pyspark.sql.functions import *

# take only month from invoice date as the year wise seperation isn't needed 
online_retail = online_retail.withColumn("month", month(online_retail['invoicedate']))
# provided calculation total_paid = unitprice * quantity
sales = online_retail.withColumn("total_paid", col("unitprice")*col("quantity"))
# aggregation of total_paid to get the sum of the articles
sales_calc = sales.groupBy("month","description").agg(sum("total_paid").alias("total_paid")).select("month","description","total_paid").alias("sc")
# aggregation of total sales grouped by month with total paid
sales_max = sales_calc.groupBy("month").agg(max("total_paid").alias("total_paid")).select("month","total_paid").alias("sm")
# joining the calculated dimensions
sales_final = (
    sales_calc.join(
        sales_max,
        (col("sc.month") == col("sm.month")) &
        (col("sc.total_paid") == col("sm.total_paid")),
        "inner"
    )
    .select(
        col("sc.month").alias("month"),
        col("sc.description").alias("description"),
        col("sm.total_paid").alias("total_paid")
    )
    .alias("odf")
)
# To validate your solution, convert your final pySpark df to a pandas df
sales_final.toPandas()
