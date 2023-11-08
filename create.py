

#    this file creates a spark session and reads the CSV to
#    pyspark dataframes


from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import when as spark_when


def create():
    # spark-context
    spark = SparkSession.builder.appName('Practical').config("spark.sql.repl.eagerEval.enabled", True).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Getting the path to the file
    path = input('''
    Please enter the path to the file:
Input :  ''')
    if(path == ''): 
        path = '.'

    # Read the CSV and apply the schema
    df_pyspark = spark.read.csv(path + '\Absence_3term201819_nat_reg_la_sch.csv', header=True, inferSchema=True)

    #Comparing this and having a look at the csv file, we find that  sess_auth_ext_holiday has invalide values that has
    #to be changed:

    df_pyspark = df_pyspark.withColumn("sess_auth_ext_holiday", spark_when(spark_col("sess_auth_ext_holiday") == ":", 0).otherwise(spark_col("sess_auth_ext_holiday")))
    
    # cast the "sess_auth_ext_holiday" column to integer
    df_pyspark = df_pyspark.withColumn("sess_auth_ext_holiday", spark_col("sess_auth_ext_holiday").cast("int"))
    return spark, df_pyspark

def create_for_streamlit(path):
    # spark-context
    spark = SparkSession.builder.appName('Practical').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Getting the path to the file
    if(path == ''): 
        path = '.'

    # Read the CSV and apply the schema
    df_pyspark = spark.read.csv(path + '\Absence_3term201819_nat_reg_la_sch.csv', header=True, inferSchema=True)

    #Comparing this and having a look at the csv file, we find that  sess_auth_ext_holiday has invalide values that has
    #to be changed:

    df_pyspark = df_pyspark.withColumn("sess_auth_ext_holiday", spark_when(spark_col("sess_auth_ext_holiday") == ":", 0).otherwise(spark_col("sess_auth_ext_holiday")))
    
    # cast the "sess_auth_ext_holiday" column to integer
    df_pyspark = df_pyspark.withColumn("sess_auth_ext_holiday", spark_col("sess_auth_ext_holiday").cast("int"))
    return spark, df_pyspark