from pyspark.sql import SparkSession

file_path= "C:\spark\color.json"
sparkp = SparkSession.builder.appName("person").getOrCreate()
df = sparkp.read.json(file_path)
df.show()
print('-----------')
df.printSchema()
print('-----------')
#df.select(['supplier']).show()
df.createOrReplaceTempView("people")
sqlDF = sparkp.sql("SELECT supplier FROM people")
sqlDF.show()
sparkp.stop()