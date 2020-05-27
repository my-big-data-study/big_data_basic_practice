import sys
import time
from argparse import ArgumentParser

import gdelt


class SaveDataToAws:
    def __init__(self, **kwargs):
        self.sink = kwargs.get('sink')
        self.sink = "output/master_file_list_parquet"

    def read_data(self):
        current_date = time.strftime('%Y %m %d', time.localtime(time.time()))
        print(current_date)
        gd = gdelt.gdelt(version=2)
        data = gd.Search([current_date], table='events', coverage=False, translation=False)
        print(data)
        # spark = SparkSession.builder \
        #     .appName('operate_data_from_aws') \
        #     .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2') \
        #     .getOrCreate()
        # spark.readStream.

    # def write_data(self, df):
    # df.to_parquet(self.sink, engine='pyarrow', compression=None, index=False)

    def run(self):
        self.read_data()
        # self.write_data(raw_df)


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--sink')

    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv):
    args_map = load_args(argv)
    spark_job = SaveDataToAws(**args_map)
    spark_job.run()


if __name__ == '__main__':
    main(sys.argv)
