from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder .master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
sentenceDataFrame = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,not neat")
    ], ["id", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
countTokens = udf(lambda words: len(words), IntegerType())
tokenized = tokenizer.transform(sentenceDataFrame)
print col("words")
tokenized.select("sentence", "words").withColumn("tokens", countTokens(col("words"))).show(truncate=False)

# +---------------------------------------+------------------------------------------+------+
# |sentence                               |words                                     |tokens|
# +---------------------------------------+------------------------------------------+------+
# |Hi I heard about Spark                 |[hi, i, heard, about, spark]              |5     |
# |I wish Java could use case classes     |[i, wish, java, could, use, case, classes]|7     |
# |Logistic,regression,models,are,not neat|[logistic,regression,models,are,not, neat]|2     |
# +---------------------------------------+------------------------------------------+------+