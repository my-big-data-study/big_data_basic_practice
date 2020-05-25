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
        data_frame.toDF("GLOBALEVENTID",
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
        data_frame.show()

    def run(self):
        self.operate_data()


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
