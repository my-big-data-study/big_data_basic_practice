import sys
from argparse import ArgumentParser

import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class UncompressAllDataJob:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        self.source = "output/master_file_list.parquet"

    def read_data(self):
        spark = SparkSession.builder.appName('uncompress_all_data_job').getOrCreate()
        return spark.read.parquet(self.source)

    @staticmethod
    def handle_csv_file(df):
        df \
            .withColumn('csv_url', col('url')) \
            .select(col('csv_url')) \
            .rdd \
            .map(lambda row: row.csv_url) \
            .foreach(lambda csv_url: {
                print(csv_url)
            })

    @staticmethod
    def read_compressed_csv(csv_url):
        csv = pandas.read_csv(csv_url,
                              compression='zip',
                              header=0, sep='\t', error_bad_lines=False)
        print(csv)

    def run(self):
        raw_data_frame = self.read_data()
        UncompressAllDataJob.handle_csv_file(raw_data_frame)
        UncompressAllDataJob.read_compressed_csv("http://data.gdeltproject.org/gdeltv2/20150218230000.export.CSV.zip")


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
