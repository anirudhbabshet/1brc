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

    print(f"Total row count {df.select(pl.len()).collect()}")
    grouped = df.group_by("station").agg(
        pl.min("temp").alias("min_temp"),
        pl.mean("temp").alias("mean_temp"),
        pl.max("temp").alias("max_temp")
    ).sort('station').collect(streaming=True)

    count = 0
    start = time.time()
    for agg_data in grouped.iter_rows():
        count = count + 1
        if time.time() - start >= 20:
            print(f"Ran for more than 10 seconds, processed {count} number of stations!!")
            print(f"{agg_data[0]}={agg_data[1]:.1f}/{agg_data[2]:.1f}/{agg_data[3]:.1f},", end=" ")
            break


if __name__ == "__main__":
    process_file("measurements.txt")
