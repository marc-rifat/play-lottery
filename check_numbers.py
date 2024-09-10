from play_mega_millions import check_combination_in_parquet

if __name__ == '__main__':
    parquet_file = 'combinations.parquet'
    main_numbers = [1, 16, 21, 47, 60]
    mega_ball = 5

    check_combination_in_parquet(parquet_file, main_numbers, mega_ball)
