from pyspark.sql import SparkSession
from typing import Optional


## https://polarpersonal.medium.com/writing-pyspark-logs-in-apache-spark-and-databricks-8590c28d1d51
## https://medium.com/@mx-max/pyspark-logging-tutorial-ef0f270b9f8

# Also

log4jLogger = sc._jvm.org.apache.log4j  # sc - spark context
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")


class LoggerProvider:
    def get_logger(self,
                   spark: SparkSession,
                   custom_prefix: Optional[str] = ""):
        log4j_logger = spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(custom_prefix + self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__
