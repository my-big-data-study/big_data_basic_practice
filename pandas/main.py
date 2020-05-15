import time

import numpy as np
import pandas as pd
from pandas import DataFrame


class SimpleWordCountExample:
    """
    source file: https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt
    save output file into output folder
    """

    def __init__(self):
        self.source_url = 'https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt'
        self.get_current_time_func = lambda: int(round(time.time() * 1000))

    def load_source_file(self) -> DataFrame:
        return pd.read_csv("../source/t8.shakespeare.txt", sep=' ', error_bad_lines=False, header=None, names=['word'])

    def do_word_count(self, df: DataFrame) -> DataFrame:
        word_counts_series = df.word.value_counts()
        word_counts_df = pd.DataFrame({'word': word_counts_series.index, 'count': word_counts_series.values})
        word_counts_df['capital'] = np.where(word_counts_df['word'].str[0].str.isalpha(),
                                             word_counts_df['word'].str[0].str.upper(), 'other')
        return word_counts_df

    def persist_result(self, df: DataFrame):
        df.to_parquet("example_pa_parquet", engine='pyarrow', partition_cols=['capital'], compression=None)

    def run(self):
        raw_df = self.load_source_file()
        wordcount_result_df = self.do_word_count(raw_df)
        self.persist_result(wordcount_result_df)


if __name__ == "__main__":
    runner = SimpleWordCountExample()
    runner.run()
