import sys
from argparse import ArgumentParser

import gdelt


class SaveDataToAws:
    def __init__(self, **kwargs):
        self.sink = kwargs.get('sink')
        self.sink = "output/master_file_list.parquet.gzip"

    def read_data(self):
        gd = gdelt.gdelt(version=2)
        return gd.Search(['2020 05 19', '2020 05 20'], table='events', coverage=True, translation=False)

    def write_data(self, df):
        df.to_parquet(self.sink, index=False, compression="gzip")

    def run(self):
        raw_df = self.read_data()
        self.write_data(raw_df)


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
