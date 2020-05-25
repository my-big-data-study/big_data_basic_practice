import os
import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession


class OperateDataFromAws:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')

    def operate_data(self):
        spark = SparkSession.builder.appName('operate_data_from_aws').getOrCreate()
        data_frame = spark.read.option("header", "false") \
            .option("delimiter", "\t") \
            .option("inferSchema", "true") \
            .csv(self.source)

        return data_frame.toDF("GLOBALEVENTID",
                               "EventTimeDate",
                               "MentionTimeDate",
                               "MentionType",
                               "MentionSourceName",
                               "MentionIdentifier",
                               "SentenceID",
                               "Actor1CharOffset",
                               "Actor2CharOffset",
                               "ActionCharOffset",
                               "InRawText",
                               "Confidence",
                               "MentionDocLen",
                               "MentionDocTone",
                               "MentionDocTranslationInfo",
                               "Extras")

    def read_data_from_elastic_search(self):
        query = """{
          "query": {
            "match_all": {}
          }  
        }"""

        spark = SparkSession.builder.appName('operate_data_from_aws').getOrCreate()

        data = spark.read \
            .format("es") \
            .option('es.nodes', '127.0.0.1') \
            .option('es.port', '9200') \
            .option('es.nodes.wan.only', 'true') \
            .option("es.net.http.auth.user", "admin") \
            .option("es.net.http.auth.pass", "HQtmh101999.") \
            .load("user")
        data.show()

    def write_data_to_elastic_search(self, df):
        df.write.format('org.elasticsearch.spark.sql') \
            .option('es.nodes', 'https://search-summer-bb3wlmxdkyv4my6xdvwk6odmnu.ap-northeast-2.es.amazonaws.com') \
            .option("es.net.http.auth.user", "admin") \
            .option("es.net.http.auth.pass", "HQtmh101999.") \
            .option('es.resource', 'gdelt/mentions') \
            .option('es.mapping.id', 'GLOBALEVENTID') \
            .option('es.write.operation', 'update') \
            .mode('append') \
            .save()

    def run(self):
        data_frame = self.operate_data()
        data_frame.show()
        # self.write_data_to_elastic_search(data_frame)
        self.read_data_from_elastic_search()


# os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
# pay attention here, jars could be added at here
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--jars /Users/mhtang/Desktop/大数据培训/data-storage-execrise/summer_project/elasticsearch-spark-20_2.11-7.7.0.jar ' \
    'pyspark-shell'


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--source')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)
    spark_job = OperateDataFromAws(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
