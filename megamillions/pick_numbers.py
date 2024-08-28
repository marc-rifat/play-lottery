import pandas as pd


def select_random_rows(parquet_file_path, num_rows):
    df = pd.read_parquet(parquet_file_path)
    if num_rows > len(df):
        raise ValueError(f"Requested number of rows ({num_rows}) exceeds the total number of rows ({len(df)}).")
    return df.sample(n=num_rows)


if __name__ == '__main__':
    df_random = select_random_rows('files/combinations.parquet', 5)
    df_random.to_csv('files/combinations.csv', index=False)
    print(df_random.to_markdown(index=False))
