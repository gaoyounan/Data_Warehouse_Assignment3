from pyspark.ml.feature import CountVectorizer
from pyspark.sql import SparkSession
# Input data: Each row is a bag of words with a ID.

spark = SparkSession.builder .master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
#spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b b c d a".split(" "))
    ], ["id", "words"])

# fit a CountVectorizerModel from the corpus.

cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=2, minDF=1.0)
model = cv.fit(df)
print model.vocabulary

model.show(truncate=100)
result = model.transform(df)

model.transform(df).show(truncate=100)

# [u'b', u'a', u'c', u'd']
# +---+---------------------+-------------------------------+
# | id|                words|                       features|
# +---+---------------------+-------------------------------+
# |  0|            [a, b, c]|      (4,[0,1,2],[1.0,1.0,1.0])|
# |  1|[a, b, b, b, c, d, a]|(4,[0,1,2,3],[3.0,2.0,1.0,1.0])|
# +---+---------------------+-------------------------------+