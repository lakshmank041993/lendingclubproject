import sys
from lib import DataManipulation,DataReader,Utils
from pyspark.sql.functions import *
from lib.logger import Log4j


if __name__ == '__main__':
    
    if len(sys.argv) <2:
        print("please specify the env ")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    spark =Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    
    logger.warn("create spark session")

    order_df = DataReader.read_orders(spark,job_run_env)
    
    order_filtered = DataManipulation.filter_closed_orders(order_df)
    
    customers_df = DataReader.read_customers(spark,job_run_env)
    
    joined_df = DataManipulation.join_orders_customers(order_filtered,customers_df)
    
    aggregate_results_df = DataManipulation.count_orders_state(joined_df)
    
    aggregate_results_df.show()
    
    logger.info("end of main")
