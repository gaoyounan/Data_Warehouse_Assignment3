from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

spark = SparkSession \
    .builder \
    .appName("readParquest") \
    .getOrCreate()


parquetFile = spark.read.parquet("result_100")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
#result = spark.sql("SELECT _c5, words, filtered, features, rawPrediction, probability, prediction FROM parquetFile")
#result = spark.sql("SELECT tweet_text, words, prediction FROM parquetFile")
result = spark.sql("SELECT tweet_text, prediction FROM parquetFile")
result.show(truncate = 1000)


#array_to_string_udf = udf(array_to_string,StringType())

#result = result.withColumn('words',array_to_string_udf(result["words"]))

file=r"test.csv"
result.write.csv(path=file, header=True, sep=",", mode='overwrite')