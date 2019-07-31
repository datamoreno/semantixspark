from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .getOrCreate()

files = ['access_log_Jul95','access_log_Aug95']

df1 = spark.read.load(files
                     , format='csv'
                     , sep=' '
                     , inferSchema='true'
                     , header='false'
                    )

df2 = df1.createOrReplaceTempView("NASA")

#### create view

query = ''' 
        CREATE OR REPLACE TEMPORARY VIEW FATO_NASA
        AS
        SELECT _c0 as host
            , FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING( concat( _c3 , _c4 ) , 2 , 20), 'dd/MMM/yyyy:HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as timestamp
            , _c5 as requisicao
            , _c6 as retorno
            , _c7 as bytes
        FROM NASA
        '''
sqlDF = spark.sql(query)

### 1. Número de hosts únicos.
sqlDF = spark.sql('SELECT COUNT(DISTINCT HOST) AS HOSTS_UNICOS FROM FATO_NASA')
sqlDF.show()

### 2. O total de erros 404.
query = ''' 
        SELECT COUNT(1) AS TOTAL_ERROS_404
        FROM FATO_NASA
        WHERE RETORNO = '404'
        '''
sqlDF = spark.sql(query)
sqlDF.show()

### 3. Os 5 URLs que mais causaram erro 404.
query = ''' 
        SELECT REQUISICAO
            , COUNT(1) AS ERROS404
        FROM FATO_NASA
        WHERE RETORNO = '404'
        GROUP BY REQUISICAO
        ORDER BY COUNT(1) DESC
        LIMIT 5
        '''
sqlDF = spark.sql(query)
sqlDF.show(truncate=False)

### 4. Quantidade de erros 404 por dia.
query = ''' 
        SELECT to_date(TIMESTAMP) AS DATA
            , COUNT(1) AS ERROS404
        FROM FATO_NASA
        WHERE RETORNO = '404'
        GROUP BY to_date(TIMESTAMP)
        ORDER BY 1 ASC
        '''
sqlDF = spark.sql(query)
sqlDF.show(100, truncate=False)

###5. O total de bytes retornados.
query = ''' 
        SELECT SUM(BYTES)*1.0/(1024*1024*1024) as GIGABYTES
        FROM FATO_NASA
        '''
sqlDF = spark.sql(query)
sqlDF.show(truncate=False)
