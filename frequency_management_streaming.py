from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#  Create a StreamingContext with two working thread and batch interval of 1 second.
sc = SparkContext("local[2]", "FlumeEventCount")
ssc = StreamingContext(sc, 1)

#  Create a flume stream.
flumeStream = FlumeUtils.createStream(ssc, "10.10.166.254", 9925)

#  Read the incoming data.
lines = flumeStream.map(lambda x: x[1])

#  Split the data (CSV).
lines = lines.map(lambda line: line.split(','))

#  Initialize Spark Session.
spark = SparkSession.builder.appName('Frequency Management').getOrCreate()

#  Create DataFrames from the RDDs.
df_customers = spark.read.format("jdbc").options(dbtable="customers_frequency").load()
df_offers    = spark.read.format("jdbc").options(dbtable="offers_frequency").load()

#  Register the DataFrames as temporary tables in Spark SQL.
df_customers.createOrReplaceTempView("customers_frequency")
df_offers.createOrReplaceTempView("offers_frequency")

def process(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd, ["sent_id","client_id","offer_id","customer_id","datetime"])
        df.write.mode('append').format("jdbc").options(dbtable="sent").save()
        
        #  Reload the sent table to include the new data.
        df_sent = spark.read.format("jdbc").options(dbtable="sent").load()
        df_sent.createOrReplaceTempView("sent")

        #  Run queries
        spark.sql("""
        INSERT INTO frequency_management
        SELECT sent.client_id, sent.customer_id
        FROM sent
        JOIN customers_frequency cust ON cust.customer_id = sent.customer_id
        JOIN offers_frequency offer ON offer.offer_id = sent.offer_id
        WHERE cust.max_frequency_total_day > (SELECT COUNT(*) FROM sent WHERE customer_id = sent.customer_id AND date(sent.datetime) = CURRENT_DATE)
        OR cust.max_frequency_total_week > (SELECT COUNT(*) FROM sent WHERE customer_id = sent.customer_id AND date(sent.datetime) >= DATE_SUB(CURRENT_DATE, 7))
        OR cust.max_frequency_offer_day > (SELECT COUNT(*) FROM sent WHERE offer_id = sent.offer_id AND date(sent.datetime) = CURRENT_DATE)
        OR cust.max_frequency_offer_week > (SELECT COUNT(*) FROM sent WHERE offer_id = sent.offer_id AND date(sent.datetime) >= DATE_SUB(CURRENT_DATE, 7))
        OR offer.max_frequency_day > (SELECT COUNT(*) FROM sent WHERE offer_id = sent.offer_id AND date(sent.datetime) = CURRENT_DATE)
        OR offer.max_frequency_month > (SELECT COUNT(*) FROM sent WHERE offer_id = sent.offer_id AND date(sent.datetime) >= DATE_SUB(CURRENT_DATE, 30))
        """)

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
