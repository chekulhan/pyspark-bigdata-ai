import pandas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
StructField("id",IntegerType(),True),
StructField("name",StringType(),True),
StructField("age",IntegerType(),True)])

# This data is what we will sample
expected_data=[(1, "Brooke", 20),
(2, "Jon", 45),
(3, "Susan", 53),
(9, "Axl", 21)] # Axl is the data row that is NOT found in actual dataset

actual_data=[(1, "Brooke", 20),
(2, "Jon", 45),
(3, "Susan", 53),
(4, "Mary", 46),
(5, "Adam", 6),
(6, "Brian", 9),
(7, "Melanie", 5),
(8, "Sarah", 10)]

expected_df = spark.createDataFrame(data=expected_data, schema=schema)
actual_df = spark.createDataFrame(data=actual_data,schema=schema)

sample_df = expected_df.sample(fraction=0.6) # get a sample of the expected dataset

pandas_df = sample_df.toPandas()
sample_df.show() # sample values that will be searched

vals = pandas_df.values
listValues = vals.tolist() # turn pandas into a list, as it is a small number of results

# Loop over the list of sampled data, checking whether it exists in the actual dataset
for line in listValues:
   count = 0
   primary_key = line[0]
   count = actual_df.filter(col("id")==primary_key).count()
   if count == 0: print("Row not found " + str(primary_key))
