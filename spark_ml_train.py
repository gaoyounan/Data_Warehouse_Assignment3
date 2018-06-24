from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import col

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

#from pyspark.ml import PipelineModel

sc =SparkContext()
sqlContext = SQLContext(sc)
data = sqlContext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load('testdata.manual.csv')

#data = data.select([column for column in data.columns if column not in drop_list])
print data.columns

data = data.selectExpr("_c0 as label", "_c5 as tweet_text")
#data.show(5)

#data.printSchema()

# data.groupBy("Sentiment") \
#     .count() \
#     .orderBy(col("count").desc()) \
#     .show()
#
# data.groupBy("SentimentText") \
#     .count() \
#     .orderBy(col("count").desc()) \
#     .show()

# set seed for reproducibility
(trainingData, testData) = data.randomSplit([0.8, 0.2], seed = 1234)
# print("Training Dataset Count: " + str(trainingData.count()))
# print("Test Dataset Count: " + str(testData.count()))

# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="tweet_text", outputCol="words", pattern="\\W")

# stop words
add_stopwords = ["http","https","amp","rt","t","c","the"]
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=40000, minDF=5)

# convert string labels to indexes
#label_stringIdx = StringIndexer(inputCol = "_c0", outputCol = "label")

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0)
#lrModel = lr.fit(trainingData)


# build the pipeline
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, lr])

# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(trainingData)
predictions = pipelineFit.transform(testData)

# predictions.filter(predictions['prediction'] == 0) \
#     .select("tweet_text","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 10, truncate = 30)
#
# predictions.filter(predictions['prediction'] == 2) \
#     .select("tweet_text","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 10, truncate = 30)
#
# predictions.filter(predictions['prediction'] == 4) \
#     .select("tweet_text","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 10, truncate = 30)

# Evaluate, metricName=[accuracy | f1]default f1 measure
#evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction",labelCol="label")
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="label")
print("F1: %g" % (evaluator.evaluate(predictions)))

# save the trained model for future use
pipelineFit.save("standford_500.logreg.model")

# PipelineModel.load("logreg.model")
