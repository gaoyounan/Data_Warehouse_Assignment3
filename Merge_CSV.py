
from pyspark import SparkContext
from pyspark.sql import SQLContext



sc = SparkContext("local[2]", "Streaming App")
sqlContext = SQLContext(sc)
df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("./result.csv/*.csv")
df.coalesce(1).write.format("com.databricks.spark.csv").csv(path="all_result", header="true", mode="append")