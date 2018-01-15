from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diverted = vuelos.map(lambda line:line.split(","))
#Dejar cabecera, elimina el header y  separa en columnas.
diverted2 = diverted.map(lambda line: (str(line[8]), float(line[23])))
diverted2 = diverted2.filter(lambda line:len(line[0]) == 3 and line[1] > 0)
totalDiverted = diverted2.reduceByKey(lambda a,b: a + b)
totalDiverted = totalDiverted.map(lambda line: ((line[1],line[0])))
totalDiverted = totalDiverted.sortByKey(False)
mostDiverted = totalDiverted.take(5)
print("\nMas Desviados\t" + str(mostDiverted))

