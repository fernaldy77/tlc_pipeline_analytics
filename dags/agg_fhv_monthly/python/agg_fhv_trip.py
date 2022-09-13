### Import all the necessary Modules
import os
import sys
import os, sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import logging
import logging.config

from datetime import datetime
from subprocess import Popen, PIPE
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from util.create_objects import get_spark_object
import util.get_all_variables as gav
from util.persist_data import persist_data_postgre

### Load the Logging Configuration File
logging.config.fileConfig(fname='/opt/airflow/dags/agg_fhv_monthly/python/util/logging_to_file.conf')

def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logging.info("load_files() is Started ...")
        df = spark. \
                read. \
                format(file_format). \
                load('hdfs://localhost:9000' + file_dir)

    except Exception as exp:
        logging.info("Error in the method - load_files(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info(f"The input File {file_dir} is loaded to the data frame. The load_files() Function is completed.")
    return df


def main():
    try:
        logging.info("main() is started ...")

        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)

        ## Read raw data
        # Get open
        month_partition = sys.argv[1]
        file_dir = f"/data/tlc_analytics/staging/{month_partition}/fhv"
        proc = Popen(['/opt/hadoop/bin/hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        file_format = 'parquet'
        header = 'NA'
        inferSchema = 'NA'

        df = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info("Ini df")
        logging.info(df.show())


        ## Transform and summary data
        df = df.withColumn('date_partition', f.to_date(col('dropOff_datetime'), 'yyyy-MM-dd H:m:s'))
        df = df.withColumn('dispatching_base_num', col('dispatching_base_num').cast(StringType())) \
            .withColumn('PUlocationID', col('PUlocationID').cast(StringType())) \
            .withColumn('DOlocationID', col('DOlocationID').cast(StringType()))

        summary_sql = df \
            .groupBy("date_partition", "dispatching_base_num", "PUlocationID", "DOlocationID") \
            .agg(f.count("dispatching_base_num").alias("count_trip"))
        summary_sql = summary_sql.withColumn('count_trip', col('count_trip').cast(IntegerType()))
        logging.info(summary_sql.printSchema())
        logging.info(summary_sql.show())

        ## Persist to postgres
        persist_data_postgre(spark=spark, df=summary_sql, dfName='agg_fhv_trip',
                             url="jdbc:postgresql://localhost:6432/tlc_analytics", driver="org.postgresql.Driver",
                             dbtable='agg_fhv_trip', mode="append", user="sparkuser1", password="user123", db_name='tlc_analytics')

        logging.info(f"agg_fhv_tripcount.py is Completed.")

    except Exception as exp:
        logging.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
              "and fix it." +str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__" :
    logging.info("agg_fhv_trip is Started ...")
    main()