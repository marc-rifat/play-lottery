from itertools import combinations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def generate_mega_millions_combinations():
    # Define ranges for the main numbers and Mega Ball
    main_numbers = range(1, 71)
    mega_ball_numbers = range(1, 26)

    # Generate all combinations of 5 main numbers
    main_combinations = list(combinations(main_numbers, 5))

    # Prepare for storing the data
    records = []

    # Generate all combinations of main numbers with each Mega Ball
    for main_comb in main_combinations:
        for mega_ball in mega_ball_numbers:
            records.append((*main_comb, mega_ball))

    # Convert to DataFrame
    df = pd.DataFrame(records, columns=['Main1', 'Main2', 'Main3', 'Main4', 'Main5', 'MegaBall'])

    # Save to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'files/combinations.parquet')


if __name__ == '__main__':
    generate_mega_millions_combinations()
