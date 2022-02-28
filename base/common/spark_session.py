import os
import sys
from pyspark.sql import SparkSession

class SparkContextSession:
    def __init__(self, app_name):
        self.__service = SparkSession \
            .builder \
            .appName(app_name or 'spark-app') \
            .getOrCreate()


    def get_service(self):
        return self.__service
