# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType)
from pyspark.sql.functions import col, asc,desc,row_number
from pyspark.sql.window import Window

# COMMAND ----------

job_start_time = datetime.now()
today_dt = job_start_time.strftime("%Y%m%d")
print(today_dt)

# COMMAND ----------

sampleEmployee= spark.read.options(header='True').format('csv').load('/FileStore/tables/us_500.csv')

# COMMAND ----------

employeeDF=sampleEmployee
for i in range(100-1):
  employeeDF = employeeDF.union(sampleEmployee)

# COMMAND ----------

employeeDF.count()

# COMMAND ----------

CityEmployeeDensity=employeeDF.groupBy('city').count().orderBy(col("count").desc())

# COMMAND ----------

temp_CityEmployeeDensity=CityEmployeeDensity.withColumn("Sequence",row_number().over(Window.orderBy(col("count").desc())))

# COMMAND ----------

display(CityEmployeeDensity)

# COMMAND ----------

VaccinationDrivePlan=employeeDF.join(other=temp_CityEmployeeDensity,on='city',how='inner')

# COMMAND ----------

display(VaccinationDrivePlan)

# COMMAND ----------

interval=0
def dt_fun(val):
  global interval
  date=(datetime.strptime(today_dt, '%Y%m%d')+ timedelta(days = int(interval))).strftime('%Y%m%d')
  if interval<100:
    interval+=1
  else:
    interval=0
  return date  

# COMMAND ----------


v_dateUDF = udf(lambda z:dt_fun(z),StringType())

# COMMAND ----------

finalVaccinationDrivePlan=VaccinationDrivePlan.withColumn("vaccination_dt", v_dateUDF(col("count")))

# COMMAND ----------

display(finalVaccinationDrivePlan)

# COMMAND ----------

final_report=CityEmployeeDensity.withColumn("no_of_days",col("count")/100)

# COMMAND ----------

display(final_report)
