# -*- coding: utf-8 -*-
#OBS: TESTEI O CODIGO NA VERSAO SPARK 1.6
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
 
if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
 
    sparkConf = SparkConf()
    sparkConf.setAppName('Access_log')
    sc = SparkContext(conf=sparkConf)
    sqlcontext = SQLContext(sc)
    
    #Lê arquivo acess_log
    rdd = sc.textFile("/user/spark/access_log*")

    print(rdd.take(5))

    #Cria data frame
    df = rdd.map(lambda x: (x, )).toDF()
    df.show(5)
    df = df.select(regexp_extract('_1', '([^ ]*)', 1).alias('host'), \
         regexp_extract('_1', ' - - \[([^\]]*)\]', 1).alias('data'), \
         regexp_extract('_1', ' "(.*?)"', 1).alias('requisicao'), \
         substring(regexp_extract('_1', '([0-9]".*)', 1), 4, 3).alias('http'), \
         substring(regexp_extract('_1', '([0-9]".*)', 1), 8, 4).alias('bytes'))

    
    #Resposta 1
    df.agg(countDistinct('host')).show()
    
    #Resposta 2
    print(df.where("http = '404'").count())

    #Resposta 3
    df.where("http = '404'").groupby('http', 'host').count().orderBy(desc('count')).show(5, False)
    
    #Resposta 4
    df2 = df.withColumn('data_formatada', substring(col("data"), 1, 11))
    df2.where("http = '404'").groupby('data_formatada').count().orderBy(desc('count')).show(5, False)    

    #Resposta 5
    print(df.select('bytes').count())
