from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple
from functools import reduce
# from pyspark.sql.functions import desc


sc = SparkContext("local[2]", "Streaming App")
pipelineFit = PipelineModel.load("standford_500.logreg.model")

sc.setLogLevel("error")
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
#ssc.checkpoint( "file:/home/ubuntu/tweets/checkpoint/")

socket_stream = ssc.socketTextStream("127.0.0.1", 5555) # Internal ip of  the tweepy streamer

lines = socket_stream.window(20)
#lines.pprint()
fields = ("tweet_text")
Tweet = namedtuple( 'Tweet', fields )


def getTotalCount():
    if ("totalTweetsCount" not in globals()):
        globals()["totalTweetsCount"] = 0
    return globals()["totalTweetsCount"]

def setTotalCount(count):
    if ("totalTweetsCount" not in globals()):
        globals()["totalTweetsCount"] = 0
    globals()["totalTweetsCount"] = count

def getSparkSessionInstance(sparkConf):
    print("before if")
    if ("sparkSessionSingletonInstance" not in globals()):
        print("before globals")
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    print("before return")
    return globals()["sparkSessionSingletonInstance"]

def do_something(time, rdd):
    print("========= %s =========" % str(time))
    try:
        print("before getSparkSessionInstance")
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        print("before Map" )
        # Convert RDD[String] to RDD[Tweet] to DataFrame
        rowRdd = rdd.map(lambda w: Tweet(w))
        print("before createDataFrame")
        linesDataFrame = spark.createDataFrame(rowRdd)

        print("before transform")
        linesDataFrame = pipelineFit.transform(linesDataFrame)

        print("before createOrReplaceTempView")
        # Creates a temporary view using the DataFrame
        linesDataFrame.createOrReplaceTempView("tweets")

        print("before spark.sql")
        # Do tweet character count on table using SQL and print it
        lineCountsDataFrame = spark.sql("select tweet_text, prediction from tweets")

        print("before lineCountsDataFrame.show()")
        #lineCountsDataFrame.show()
        #lineCountsDataFrame.coalesce(1).write.mode("append").format("com.databricks.spark.csv").save("dirwithcsv", mode="append")
        lineCountsDataFrame.coalesce(1).write.format("com.databricks.spark.csv").csv(path="result.csv", header="true", mode="append")

        totalCount = getTotalCount()
        totalCount = totalCount + lineCountsDataFrame.count()
        setTotalCount(totalCount)
        if totalCount >= 300:
            ssc.stop()


    except:
        pass


# key part!
lines.foreachRDD(do_something)

ssc.start()
ssc.awaitTermination()