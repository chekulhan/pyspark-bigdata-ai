
from pyspark.sql.functions import count, when, trim, length, lit, lpad, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

actual_data=[("Brooke", 20.00, "000000145", "MAC",0,"N"), 
      ("Brooke ", 21.00, "000000145", "MAC",0,"N"),
      ("Brooke", 35.00, "000000145", "CBA",0, "N"),
      ("   Brooke", 12.00, "000000145" ,"CBA",0, "N"),
      ("Denny", 31.00, "000000323", "CBA",0, "N"), 
      ("Jules    ", 30.00, "000000658" ," CBA ",0, "N"), 
      ("TD", 35.00, "00323", " CBA ",0, None), 
      ("TD", 45.00, "00323", "AAA ",6, "N"), 
      ("Brooke", 99.00,  "000000145" ,"CBA", None, None)]

actual_schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("balance",DoubleType(),True), 
    StructField("acct",StringType(),True), 
    StructField("org", StringType(), True), 
    StructField("status", IntegerType(), True), 
    StructField("valid", StringType(), True)
  ])

actual_df = spark.createDataFrame(data=actual_data, schema=actual_schema)

actual_df.printSchema()

cols = ["name", "org"] # define columns to be tested

# The following functions are tests on individual datasets. No test dataset or comparison dataset is used.
# If results are returned, it means that the column is not trimmed correctly. 
def test_trimvalues(df, cols):
  for col in cols:
    (df
      .select(col)
      .filter(length(trim(col)) != length(col))
      .withColumn("TRIM Test on column", lit(col))
      .show()
    )

test_trimvalues(actual_df, cols)

# Pad the account number as a new field and compare it to the actual dataset
# No test dataset used
# If results are returned, it means that the column is not padded correctly.

def test_accountnumber(df, field):
  LENGTH= 9 # acct length to be padded
  PADDED_CHARACTER = "0"
  
  (df
    .select(field)
    .withColumn('acct_padded', lpad(field, LENGTH, PADDED_CHARACTER))
    .filter(col(field) != col("acct_padded"))
    .show()
  )
    

test_accountnumber(actual_df, "acct")


def test_integerType(df, field):

  (df
    .selectExpr("cast(status as int) status_type")
    #.withColumn('acct_padded', lpad(field, LENGTH, PADDED_CHARACTER))
    .filter(col(field) != col("status_type"))
    .show()
  )
    

test_integerType(actual_df, "status")

#actual_df.withColumn("status_trim", when(length(trim(col("name"))) > col("name"), "NOT OK")
#  .otherwise("OK")
#  ).show()


# Original column was NULL, therefore the business rule states that this colun should be ZERO(0)
# No test dataset used
# If results are returned, it means that the column is not ZERO correctly.

def test_nulltozero(df, fields):
  for field in fields:
    (df
      .select(field, "*", )
      .filter(col(field).isNull())
      .show()
    )

test_nulltozero(actual_df, ["status", "valid"])
