# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Start writing code
sf_events = sf_events.withColumn("lead",lead(col("record_date"),2).over(Window         .partitionBy("user_id").orderBy("record_date")).alias("lead"))\
            .withColumn('addtn', dateadd(sf_events.record_date, 2))\
            .filter(col('addtn') == col('lead'))\
            .select(col('user_id'))
# sf_events.show()
# To validate your solution, convert your final pySpark df to a pandas df
sf_events.toPandas()
# sf_events.info()
