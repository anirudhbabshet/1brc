import time
import polars as pl
import sys


def process_file(file):

    df = pl.scan_csv(
        file,
        separator=";",
        has_header=False,
        with_column_names=lambda cols: ["station", "temp"]
    )

    grouped = df.group_by("station").agg(
        pl.min("temp").alias("min_temp"),
        pl.mean("temp").alias("mean_temp"),
        pl.max("temp").alias("max_temp")
    ).sort('station').collect(streaming=True)

    start = time.time()
    for agg_data in grouped.iter_rows():
        print(f"{agg_data[0]}={agg_data[1]:.1f}/{agg_data[2]:.1f}/{agg_data[3]:.1f},", end=" ")


if __name__ == "__main__":
    process_file("measurements.txt")
