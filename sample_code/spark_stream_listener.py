from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from pyspark.streaming import StreamingContext

# from pyspark.sql.functions import desc

totalCount = 0
countTotalTweets = udf(lambda count: count + totalCount, IntegerType())
sc = SparkContext("local[2]", "Tweet Streaming App")
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 10)
#sqlContext = SQLContext(sc)
ssc.checkpoint( "file:/Users/gaoyounan/Desktop/server/tweets/checkpoint/")

lines = ssc.socketTextStream("127.0.0.1", 5555) # Internal ip of  the tweepy streamer

#lines = socket_stream.window(20)

lines.pprint()

lines.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#lines.count().map(lambda x: totalCount = totalCount + x).pprint()
# If we want to filter hashtags only
# .filter( lambda word: word.lower().startswith("#") )
words = lines.flatMap( lambda twit: twit.split(" ") )
pairs = words.map( lambda word: ( word.lower(), 1 ) )
wordCounts = pairs.reduceByKey( lambda a, b: a + b ) #.transform(lambda rdd:rdd.sortBy(lambda x:-x[1]))
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
