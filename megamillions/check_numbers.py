import pandas as pd


def check_combination_in_parquet(parquet_file, main_numbers, mega_ball):
    df = pd.read_parquet(parquet_file)
    match = df[
        (df['Main1'] == main_numbers[0]) &
        (df['Main2'] == main_numbers[1]) &
        (df['Main3'] == main_numbers[2]) &
        (df['Main4'] == main_numbers[3]) &
        (df['Main5'] == main_numbers[4]) &
        (df['MegaBall'] == mega_ball)
        ]

    if not match.empty:
        print("The combination is in the file.")
    else:
        print("The combination is NOT in the file.")


main_numbers_to_check = [28, 30, 44, 66, 69]
mega_ball_to_check = 2

check_combination_in_parquet('files/combinations.parquet', main_numbers_to_check, mega_ball_to_check)
