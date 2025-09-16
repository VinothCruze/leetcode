# Import your libraries
import pyspark.sql.functions as F
# filter orders data
orders = orders.filter(F.col('order_date').between('2019-02-01' ,'2019-05-01'))
# aggregate orders per date
daily_customer_totals = orders.groupBy("cust_id","order_date").agg(F.sum("total_order_cost").alias("sum_cost")).alias("dct")
# get maximum value from the sum aggregated
daily_max = daily_customer_totals.groupBy("order_date").agg(F.max("sum_cost").alias("max_cost")).alias("dm")
# Join to keep only customers with the max cost for each day
orders_df = (
    daily_customer_totals.join(
        daily_max,
        (F.col("dct.order_date") == F.col("dm.order_date")) &
        (F.col("dct.sum_cost") == F.col("dm.max_cost")),
        "inner"
    )
    .select(
        F.col("dct.cust_id").alias("cust_id"),
        F.col("dct.order_date").alias("order_date"),
        F.col("dm.max_cost").alias("max_cost")
    )
    .alias("odf")
)
# join two df with dept_id
join_df = customers.join(orders_df, customers["id"] == orders_df["cust_id"],"inner").select("first_name","order_date","max_cost")

# To validate your solution, convert your final pySpark df to a pandas df
join_df.toPandas()
join_df.head(20)
