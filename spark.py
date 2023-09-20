from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os
from dotenv import load_dotenv
import os

load_dotenv()

pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PW")


spark = SparkSession.builder.appName("airflow").getOrCreate()

table_names = ["airflow_input2"]
table_dataframes = {}

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

for table_name in table_names:
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    
    table_dataframes[table_name] = df

main_df = table_dataframes["airflow_input2"]
main_df.show()

filtered_df = df.filter(df["language"] == 'en')

filtered_df.show()

filtered_df.write.parquet("/home/rojesh/Documents/csf/parquet/output.parquet")


filtered_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'answer1', user=pg_user,password=pg_password).mode('overwrite').save()