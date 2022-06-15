# if using google colab
# CELL 1: 
!pip install pyspark

# CELL 2: 
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Colab").config('spark.ui.port', '4050').getOrCreate()

from pyspark.sql.functions import count, when, trim, length, lit, lpad, col, to_date, substring,date_format, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, DecimalType
from datetime import datetime

# CELL 3: 

expected_data=[("Brooke", "20.91", "000000145", "MAC",None,"N", None, "19760201", "19760201"), 
      ("TD", "45", "00323", None, 10, "N",  "14/12/2021", "19770529", "19770529"),
      ("ABC ", None, "00323", "XXX", 6, "N",  "14/12/2021", None, None)]

expected_schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("balance",StringType(),True), 
    StructField("acct",StringType(),True), 
    StructField("org", StringType(), True), 
    StructField("status", StringType(), True), 
    StructField("valid", StringType(), True) ,
    StructField("date", StringType(), True) ,
    StructField("dob", StringType(), True) ,
    StructField("enddate", StringType(), True)
  ])
 

actual_data=[("Brooke", 20.91, "000000145", "MAC",0,"N", datetime(1976, 2, 1), datetime(1976, 2, 1)), 
      ("ABC", 0.00, "00323", "XYZ", 6, "N", None, None),
      ("TD", 45.00, "00323", "AAA",99, "N", datetime(1977, 5, 29), datetime(1977, 5, 29))]

actual_schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("balance",DoubleType(), True), 
    StructField("acct",StringType(),True), 
    StructField("org", StringType(), True), 
    StructField("status", IntegerType(), True), 
    StructField("valid", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("enddate", DateType(), True)
  ])

expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
actual_df = spark.createDataFrame(data=actual_data, schema=actual_schema)
expected_df.printSchema()
actual_df.printSchema()

# CELL 4:
# date time formatting - https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

dictMapping = {
    "name": "trim_rule",
    "balance": "decimal_rule",
    "org": "org_rule",
    "status": "status_rule",
    "dob": "dob_rule",
    "acct": "",
    "enddate": "enddate_rule"
}

listColumns = list(dictMapping.keys())

# select all fields in above list - to do, remove order by
transformed_expected_df = expected_df.select(listColumns).orderBy(col("name")) # order by only here to make it easier to view
transformed_actual_df = actual_df.select(listColumns).orderBy(col("name"))

# transform the actual dataset for this demo, so that have correct format and datatype
transformed_actual_df = transformed_actual_df.withColumn("dob", date_format(col("dob"), "y"))
transformed_actual_df = transformed_actual_df.withColumn("balance", col("balance").cast(DecimalType(5,2)))

#apply transformation rule to TEST/EXPECTED dataset
for field,value in dictMapping.items():
  if value =="org_rule":
    # get transformation rule
    conditions_org = when(col(field).isNull(), "AAA").when(col(field)=="XXX", "XYZ").otherwise(col(field))
    # apply transformation rul
    transformed_expected_df = transformed_expected_df.withColumn(field, conditions_org)
  elif value == "dob_rule":
    conditions_dob = when(col(field).isNull(), None).otherwise(substring(col(field), 1, 4))
    transformed_expected_df = transformed_expected_df.withColumn(field, conditions_dob)
  elif value == "status_rule":
    conditions_status = when(col(field).isNull(), 0).when(col(field)==10, 99).otherwise(col(field))
    transformed_expected_df = transformed_expected_df.withColumn(field, conditions_status)
  elif value == "trim_rule":
    transformed_expected_df = transformed_expected_df.withColumn(field, trim(col(field)))
  elif value == "decimal_rule":
    conditions_decimal = when(col(field).isNull(), 0.00).otherwise(col(field)) # must use otherwise, as it populates other columns with NULL if not
    transformed_expected_df = transformed_expected_df.withColumn(field, conditions_decimal)
    transformed_expected_df = transformed_expected_df.withColumn(field, col(field).cast(DecimalType(5,2)))
  elif value =="enddate_rule":
    conditions_enddate = when(col(field).isNull(), None).otherwise(concat(substring(col(field),1, 4), lit("-"), substring(col(field),5, 2), lit("-"), substring(col(field),7, 2)))
    transformed_expected_df = transformed_expected_df.withColumn(field, conditions_enddate)
  else:
    transformed_expected_df = transformed_expected_df.withColumn(field, col(field))

transformed_expected_df.show(10, False)
transformed_actual_df.show(10, False)

subtracted_df = transformed_actual_df.subtract(transformed_expected_df)
print("subtracted:")
subtracted_df.orderBy(col("name")).show()
