from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# logFile = "README.md"  # Should be some file on your system
# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# logData = spark.read.text(logFile).cache()

# numAs = logData.filter(logData.value.contains('a')).count()
# numBs = logData.filter(logData.value.contains('b')).count()

# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

# spark.stop()

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import StringType, IntegerType

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder \
  .appName("TigerGraphAnalysis") \
  .config("spark.driver.extraClassPath", "/usr/local/Cellar/apache-spark/3.2.1/libexec/jars/*:tigergraph-jdbc-driver-1.3.0.jar") \
  .getOrCreate()

# read vertex
jdbcDF = spark.read \
  .format("jdbc") \
  .option("driver", "com.tigergraph.jdbc.Driver") \
  .option("url", "jdbc:tg:http://127.0.0.1:14240") \
  .option("user", "tigergraph") \
  .option("password", "tigergraph") \
  .option("graph", "AMLSim") \
  .option("dbtable", "vertex Transaction") \
  .option("limit", "1000") \
  .option("debug", "0") \
  .load()

jdbcDF.show()

# Read Transaction Vertices -> Read 1000 of the transactions from the TG DB in Spark
jdbcDF1 = spark.read \
    .format("jdbc") \
    .option("driver", "com.tigergraph.jdbc.Driver") \
    .option("url", "jdbc:tg:http://127.0.0.1:14240") \
    .option("user", "tigergraph") \
    .option("password", "tigergraph") \
    .option("graph", "AMLSim") \
    .option("dbtable", "vertex Transaction") \
    .option("limit", "1000") \
    .option("debug", "0") \
    .load()

jdbcDF1.show()

# Read Edges -> Get all the transaction vertices connected to an account (9934) via the reverse_SEND_TRANSACTION edge
jdbcDF2 = spark.read \
    .format("jdbc") \
    .option("driver", "com.tigergraph.jdbc.Driver") \
    .option("url", "jdbc:tg:http://127.0.0.1:14240") \
    .option("user", "tigergraph") \
    .option("password", "tigergraph") \
    .option("graph", "AMLSim") \
    .option("dbtable", "edge reverse_SEND_TRANSACTION") \
    .option("limit", "1000") \
    .option("source", "9934") \
    .option("debug", "0") \
    .load()

jdbcDF2.show()

# Run Query -> Run a query to select an account and view account's transactions
jdbcDF3 = spark.read \
    .format("jdbc") \
    .option("driver", "com.tigergraph.jdbc.Driver") \
    .option("url", "jdbc:tg:http://127.0.0.1:14240") \
    .option("user", "tigergraph") \
    .option("password", "tigergraph") \
    .option("graph", "AMLSim") \
    .option("dbtable", "query selectAccountTx(acct=9934)") \
    .option("debug", "0") \
    .load()
jdbcDF3.show()