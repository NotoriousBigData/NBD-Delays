from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diasTiempos = vuelos.map(lambda line:line.split(","))
#Dejar cabecera, elimina el header y  separa en columnas.


#Calculamos num de retrasos por aeropuerto.
diasTiempos2 = diasTiempos.map(lambda line:((str(line[7]),str(line[11]))))
diasTiempos2 = diasTiempos2.filter(lambda line: len(line[0]) == 3 and len(line[1]) > 0)
diasTiempos3 = diasTiempos2.filter(lambda line: float(line[1]) > 10).map(lambda line:(line[0],1))
diasTiempo4 = diasTiempos3.reduceByKey(lambda a,b: a +b)


#Calculamos los vuelos x aeropuerto.
originFlights = diasTiempos.map(lambda line: (str(line[7]), 1))
originFlights = originFlights.filter(lambda line: len(line[0]) == 3)
originFlights = originFlights.reduceByKey(lambda a,b: a + b)


#Ordenamos
diasTiempo4 = diasTiempo4.sortByKey()
originFlights = originFlights.sortByKey()

listaVuelos = originFlights.collect()
listaRetrasos = diasTiempo4.collect()

max = diasTiempo4.count()

result = {}

for x in range(0, max):
    result[listaVuelos[x][0]] = ((float(listaRetrasos[x][1]) / float(listaVuelos[x][1])) * 100)
 
datos = []

for w in sorted(result, key= result.get):
     datos.append(str(w) + " "+ str(result[w]) + "% ")


print("Los aeropuertos con menos tasa de retraso son: " + datos[0] + datos[1] + datos[2] + datos[3] + datos[4])
print("Los aeropuertos con mas tasa de retraso son: " + datos[317] + datos[318] + datos[319] + datos[320] + datos[321])
