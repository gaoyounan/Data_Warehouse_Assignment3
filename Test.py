from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import col

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc =SparkContext()
sqlContext = SQLContext(sc)
data = sqlContext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load('Tweets.csv')

data = data.selectExpr("_c0 as label")

data.groupBy("label") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print data.count()