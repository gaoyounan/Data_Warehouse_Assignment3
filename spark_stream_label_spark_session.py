from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from collections import namedtuple


# def IsOpen(ip,port):
#     s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#     try:
#         s.connect((ip,int(port)))
#         s.shutdown(2)
#
#         print '%d is open' % port
#         return True
#     except Exception:
#         print Exception
#         print '%d is down' % port
#         return False

totalCounts = 0

spark = SparkSession \
    .builder \
    .appName("LabelTheStreamTweets") \
    .getOrCreate()


pipelineFit = PipelineModel.load("standford_500.logreg.model")


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 5555) \
    .load()

lines.printSchema()
lines = lines.selectExpr("value as tweet_text")
lines.printSchema()

fields = ("tweet_text")
Tweet = namedtuple( 'Tweet', fields )

countTotalTweets = udf(lambda count: count + totalCounts, IntegerType())

predictions = pipelineFit.transform(lines)

def do_something(time, rdd):
    print("========= %s =========" % str(time))
    try:


        # Convert RDD[String] to RDD[Tweet] to DataFrame
        rowRdd = rdd.map(lambda w: Tweet(w))
        linesDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        linesDataFrame.createOrReplaceTempView("tweets")

        # Do tweet character count on table using SQL and print it
        lineCountsDataFrame = spark.sql("select SentimentText, length(SentimentText) as TextLength from tweets")
        lineCountsDataFrame.show()
        lineCountsDataFrame.coalesce(1).write.format("com.databricks.spark.csv").save("dirwithcsv")
    except:
        pass

file_save = predictions \
 .writeStream \
 .foreachRDD(do_something)


file_save.awaitTermination()

# file_save = predictions \
#  .writeStream \
#  .format("parquet") \
#  .option("path", "result_100") \
#  .option("checkpointLocation", "result_100")\
#  .outputMode("append") \
#  .start()
# file_save.awaitTermination()

# predictions.withColumn("totalCounts", countTotalTweets(lines.count())).count("*")
#
#
# words = predictions.select("_c5")
#
# # Generate running word count
# wordCounts = words.groupBy("_c5").count()
# wordCounts.select("count").show()

# sq = predictions.writeStream.format('memory').outputMode("append").queryName('this_query').trigger(processingTime='5 seconds').start()
# spark.sql("select * from this_query").show();
# sq.awaitTermination()

# def run(param1, param2):
#     while True:
#         sleep(10)
#         print("I am Thread1" + param1)
#         if IsOpen("100.69.17.35", 55555)== False:
#             file_save.stop()
#             break
#
# thread1 = threading.Thread(target=run, name="Thread1", args=("123", "123"))
# thread1.start()





# predictions.select("prediction")\
#  .writeStream \
#  .format("text") \
#  .option("path", "result_1000") \
#  .option("checkpointLocation", "result_1000")\
#  .outputMode("append") \
#  .start()


# console_output = predictions.writeStream.outputMode("append").format("console").start()
# console_output.awaitTermination()






#lines.writeStream.format("console").start()

# lines.printSchema()
#
# query = lines \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()
#
# query.awaitTermination()

