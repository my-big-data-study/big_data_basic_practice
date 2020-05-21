import sys
from argparse import ArgumentParser

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class SaveAllData:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        self.sink = kwargs.get('sink')

    def read_data(self):
        spark = SparkSession.builder.appName('save_all_data').getOrCreate()
        schema = StructType([
            StructField('id', StringType()),
            StructField('number', StringType()),
            StructField('url', StringType())
        ])
        return spark.createDataFrame(
            pd.read_csv(self.source, sep=' ', header=None, names=['id', 'number', 'url']),
            schema=schema
        )

    def write_data(self, df):
        df.coalesce(1).write.mode('overwrite').option('header', True).csv(self.sink)

    def run(self):
        raw_df = self.read_data()
        self.write_data(raw_df)


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--source')
    parser.add_argument('--sink')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)
    spark_job = SaveAllData(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
