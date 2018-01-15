from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
distanciaRetrasos = vuelos.map(lambda line:line.split(","))
distanciaRetrasos_2 = distanciaRetrasos.map(lambda line:(float(line[17]),(str(line[11]),str(line[22]))))
distanciaRetrasos_3 = distanciaRetrasos_2.filter(lambda line: len(line[1][0])>0 and len(line[1][1])>0)
def isDelayed(x,y):
	if x > 10 or y > 10:
		return 1
	else:
		return 0

distanciaRetrasos_4 = distanciaRetrasos_3.map(lambda line:( line[0], isDelayed(float(line[1][0]),float(line[1][1]))))
distanciaRetrasos_5 = distanciaRetrasos_4.filter(lambda line:line[1] > 0)
distanciaRetrasos_5 = distanciaRetrasos_5.reduceByKey(lambda a,b: a + b)
distanciaRetrasosTodos = distanciaRetrasos_5.sortByKey(False)

lenDR = len(distanciaRetrasosTodos.collect())
minDist = distanciaRetrasosTodos.collect()[lenDR-1][0]
maxDist = distanciaRetrasosTodos.collect()[0][0]
media = (minDist + maxDist) / 2
c1 = (minDist + media) / 2
c2 = media
c3 = (media + maxDist) / 2 
rangos = {'1': str(minDist)+ "-" + str(c1),'2': str(c1)+ " - " + str(c2),'3': str(c2)+ " - " + str(c3), '4': str(c3)+ " - " + str(maxDist) }
def getRango(x):
     if x >= minDist and x < c1:
             return 1
     if x >= c1 and x < c2:
             return 2
     if x >= c2 and x < c3:
             return 3
     if x >= c3:
             return 4
 
rangoDistanciaRetrasos = distanciaRetrasosTodos.map(lambda line: (getRango(line[0]),line[1]))
rangoRetrasos = rangoDistanciaRetrasos.reduceByKey(lambda a,b: a + b)

nVuelosRango = distanciaRetrasos_3.map(lambda line:( float(line[0]),1))
nVuelosRango_2 = nVuelosRango.reduceByKey(lambda a,b: a + b)
nVuelosRango_2 = nVuelosRango_2.map(lambda line: (getRango(line[0]),line[1]))
nTotalVuelosRango = nVuelosRango_2.reduceByKey(lambda a,b: a +b)

aRR = rangoRetrasos.collect()
aTVR = nTotalVuelosRango.collect()
result = [((aRR[0][1] * 100) / aTVR[0][1]) , ((aRR[1][1] * 100) / aTVR[1][1]), ((aRR[2][1] * 100) / aTVR[2][1]), ((aRR[3][1] * 100) / aTVR[3][1])]
print ("Rango 1 ---> [" + str(rangos['1']) + "] millas tiene una tasa de porcenataje de retraso del " + str(result[0]) + "%\n")
print ("Rango 2 ---> [" + str(rangos['2']) + "] millas tiene una tasa de porcenataje de retraso del " + str(result[1]) + "%\n")
print ("Rango 3 ---> [" + str(rangos['3']) + "] millas tiene una tasa de porcenataje de retraso del " + str(result[2]) + "%\n")
print ("Rango 4 ---> [" + str(rangos['4']) + "] millas tiene una tasa de porcenataje de retraso del " + str(result[3]) + "%\n")

