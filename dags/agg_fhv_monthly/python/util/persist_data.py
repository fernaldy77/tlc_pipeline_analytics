import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config

logger = logging.getLogger(__name__)

def persist_data_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password, db_name):
    try:
        logger.info(f"Data Persist Postgre Script  - data_persist_rdbms() is started for saving dataframe "+ dfName + " into Postgre Table...")
        #spark.sql(f""" use {db_name} """)
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode('append') \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.info("Error in the method - data_persist_postgre(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Data Persist Postgre- data_persist_postgre() is completed for saving dataframe "+ dfName +" into Postgre Table...")

