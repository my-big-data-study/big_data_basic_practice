import sys
from argparse import ArgumentParser

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType


class AwsSparkJob:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        self.sink = kwargs.get('sink')

    def read_data(self):
        spark = SparkSession.builder.appName('aws_spark_job').getOrCreate()
        schema = StructType([
            StructField('word', StringType())
        ])

        return spark.read.format("csv") \
            .option("delimiter", "\n") \
            .schema(schema) \
            .load(self.source)

    @staticmethod
    def transform(df):
        return df.withColumn('word', F.explode(F.split('word', ' '))) \
            .withColumn('word', F.regexp_extract('word', r'(\b[a-zA-Z0-9]+\b)', 1)) \
            .filter(F.col('word') != '')

    @staticmethod
    def calculate(df):
        return df.groupby('word').count().orderBy(F.desc('count'))

    def write_data(self, df):
        df.coalesce(1).write.mode('overwrite').option('header', True).csv(self.sink)

    def run(self):
        raw_df = self.read_data()
        transformed_df = AwsSparkJob.transform(raw_df)
        result_df = AwsSparkJob.calculate(transformed_df)
        self.write_data(result_df)


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--source')
    parser.add_argument('--sink')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)

    spark_job = AwsSparkJob(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
