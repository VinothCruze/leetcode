from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.window import Window
from pyspark.sql.functions import collect_list

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
#given data and dataframe creation 
data = [('employee1', 'this employee joined four years back in the filed of data analytics as a data engineer and still he is working in the same department'), 
        ('employee2', 'this employee has resigned from his role a few days back'), 
        ('employee3', ' this employee got hike from his normal salary to a hike of twentee percentage'), 
        ('employee4', 'a employee who has lot of technical knowledge with just two years of experience with a lot of exposer in bulding data architecture')]
schema = ['employee', 'details']
df = spark.createDataFrame(data, schema= schema)


x = df.withColumn('words', split(col('details'), ' '))
# display(x)

y = x.select(col('employee'), explode(col('words')).alias('words'))

z = y.groupBy('words').agg(count('words').alias('numberof_occurances'))

wind_sp = Window.orderBy(col('numberof_occurances').desc())
a = z.withColumn('drnk', dense_rank().over(wind_sp))

b = a.where(col('drnk') == 3).select(col('words'))



b.groupBy().agg(collect_list('words').alias('list_of_words')).display()
