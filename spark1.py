"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "C:\spark\README.md"  # Should be some file on your system
print(logFile)
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()
logData.show()
numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()