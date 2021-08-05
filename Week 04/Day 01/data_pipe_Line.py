import sys
from pyspark.sql import SparkSession
from pipeline import transform, persist, ingest
import logging
import logging.config


class Pipeline:
    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def run_pipeline(self):
        try:
            logging.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark)
            df = ingest_process.ingest_data()
            df.show(5)
            #for i in range(len(df)):
            #    df[i].show(5)

        except Exception as exp:
            logging.error(
                "An error occured while running the pipeline > " + str(exp))
            sys.exit(1)

        return

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("my first spark app")\
            .config("spark.driver.extraClassPath", "pipeline/postgresql-42.2.18.jar")\
            .enableHiveSupport().getOrCreate()


if __name__ == '__main__':
    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')