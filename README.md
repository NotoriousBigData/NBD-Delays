# NBD-Delays
Análisis exhaustivo de los vuelos hechos en EEUU durante el año 2015. En este proyecto, analizamos una gran cantidad de datos para responder algunas preguntas sobre todos esos vuelos.
## Datos
Los datos que hemos utilizado han sido extraidos de la página Kaggle : https://www.kaggle.com/usdot/flight-delays .
Se divide en tres archivos. El primero contiene todos los vuelos sucecidos en América del Norte durante 2015, con 31 columnas que contienen metadatos sobre éstos (origen, destino, tiempo de llegada, causas retraso, etc.). El segundo lista todos los aeropuertos que podemos encontrar en el data-set con su abreviación. El tercero contiene todas las aerolineas que realizaron vuelos durante ese año.
En su conjunto , tienen un tamaño total de 600MB.

## Código
El código está compuesto por vario Scripts escritos en lenguaje Python utilizando la herramienta Spark. Estos realizan una serie de operaciones sobre el archivo "flights.csv" y como salida escribe una serie de resultados en la consola.
## Ejecución
Para poder ejecutar los Scripts, antes han de haberse descargado el data-set desde el enlace arriba descrito. Una vez descargados los archivos, han de extraerse y añadirse a la carpeta en la que se encuentran los Scripts. Existen dos opciones para ejecutarlos:

 - Ejecutar Spark 2.2.0 o superior. Una vez en el contexto de Spark,   
    copiar el Script y pegarlo en la consola.
    
 - Dentro de la carpeta de Spark, acceder al directorio /bin y ejecutar el siguiente comando:
	**./spark-submit  "nombreScript"**
## Presentación
https://prezi.com/view/6warzWL4fjVbt2uvP7xo/
## Web
https://notoriousbigdata.github.io/
