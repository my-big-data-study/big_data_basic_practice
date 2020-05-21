import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession


class UncompressAllDataJob:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')

    def read_data(self):
        spark = SparkSession.builder.appName('uncompress_all_data_job').getOrCreate()
        return spark.read.parquet(self.source)

    def run(self):
        self.read_data().show()


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--source')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)

    spark_job = UncompressAllDataJob(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
