import sys
from argparse import ArgumentParser

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class SaveAllSingleData:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        self.source = "../output/master_file_list.parquet"

    def read_data(self):
        spark = SparkSession.builder.appName('save_all_single_data').getOrCreate()
        return spark.read.parquet(self.source)

    @staticmethod
    def handle_csv_file(df):
        urls = df.withColumn('csv_url', col('url')).select(col('csv_url')).rdd.map(lambda row: row.csv_url).collect()
        for url in urls:
            file_name = url.split("/")[-1]
            if file_name.endswith('mentions.CSV.zip'):
                response = requests.get(url)
                with open("../data/" + file_name, 'wb') as file:
                    file.write(response.content)

    def run(self):
        raw_data_frame = self.read_data()
        SaveAllSingleData.handle_csv_file(raw_data_frame)


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--source')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)

    spark_job = SaveAllSingleData(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
