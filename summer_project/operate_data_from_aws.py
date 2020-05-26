import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession


class OperateDataFromAws:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        self.es_username = "admin"
        self.es_password = "HQtmh101999."
        self.source = "data/*.CSV"

    def operate_data(self):
        spark = SparkSession.builder \
            .appName('operate_data_from_aws') \
            .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2') \
            .getOrCreate()

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
        spark = SparkSession.builder \
            .appName('operate_data_from_aws') \
            .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2') \
            .getOrCreate()

        query = """{   
                     "query": {
                        "match": {
                          "GLOBALEVENTID":"474969244"
                        }
                      }
                    }"""

        data = spark.read \
            .format("es") \
            .option('es.nodes', 'http://localhost') \
            .option('es.port', '9200') \
            .option("es.query", query) \
            .option('es.nodes.wan.only', 'true') \
            .option("es.net.http.auth.user", self.es_username) \
            .option("es.net.http.auth.pass", self.es_password) \
            .load("gdelt")
        data.show()

    def write_data_to_elastic_search(self, df):
        df.write.format('org.elasticsearch.spark.sql') \
            .option('es.nodes', 'http://localhost') \
            .option('es.port', '9200') \
            .option('es.nodes.wan.only', 'true') \
            .option("es.net.http.auth.user", self.es_username) \
            .option("es.net.http.auth.pass", self.es_password) \
            .option('es.mapping.id', 'GLOBALEVENTID') \
            .mode('append') \
            .save("gdelt/mentions")

    def run(self):
        data_frame = self.operate_data()
        self.write_data_to_elastic_search(data_frame)
        self.read_data_from_elastic_search()


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
