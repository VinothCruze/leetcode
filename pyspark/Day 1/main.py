# Import your libraries
import pyspark.sql.functions as F
# join two df with dept_id
join_df = db_employee.join(db_dept, db_employee["department_id"] == db_dept["id"],"inner").select("salary","department")
#filter only marketing and engineering departments
dept = ['marketing', 'engineering']
filtered_df = join_df.filter(F.col('department').isin(dept))
# pivot with the department's max salary
pivot_df = filtered_df.groupBy().pivot("department").agg(F.max("salary").alias("MAX_OF_SALARY"))
# abs difference between two maximum salaries
final = pivot_df.withColumn("salary_difference", F.abs(F.col('marketing')-F.col('engineering'))).select("salary_difference")
# # To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
