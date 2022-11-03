
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit, col, when, expr

schema = StructType([ 
    StructField("id",IntegerType(),True), 
    StructField("name",StringType(),True), 
    StructField("email",StringType(),True)])


data_emails=[(1, "Brooke", "brooke@yahoo.com"), 
      (2, "Jon", "jon@hotmail.com"),
      (3, "Susan", "susan@gmail.com"),
      (4, "Axl", "None"),
      (5, "Adam", None)]

df_emails = spark.createDataFrame(data=data_emails, schema=schema)

df_emails.show(5, False)

conditions_mask = when(col("email").isNotNull(), lit("***Masked***")).otherwise(col("email"))
df_emails = df_emails.withColumn("email", conditions_mask)

df_emails.show(5, False)

# data encryption using aes
df_emails = df_emails.withColumn("encrypted_email", expr("hex(aes_encrypt(email, '1234567890abcdef', 'GCM'))"))
df_emails.show(5, False)

df_emails = df_emails.withColumn("decrypted_email", expr("aes_decrypt(unhex(encrypted_email), '1234567890abcdef', 'GCM')").cast(StringType()))
df_emails.show(10, False)
