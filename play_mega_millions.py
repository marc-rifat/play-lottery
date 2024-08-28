import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from itertools import combinations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

log_file_path = os.path.join(os.path.dirname(__file__), 'logger.log')
with open(log_file_path, 'w') as file:
    pass
logging.basicConfig(filename=log_file_path, filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

THINK_TIME = 15
PLAYS = 10
URL = 'https://www.valottery.com/data/draw-games/megamillions'

DATE_XPATH = "//h3[@class='title-display']"

XPATHS = [
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[1]",
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[2]",
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[3]",
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[4]",
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[5]",
    "/html/body/div[1]/div/div[2]/div[1]/div/div/div/div[1]/div[1]/div/ul/li[6]/div/span"
]


def initialize_browser():
    options = ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('window-size=2560x1440')
    options.add_argument('--disable-blink-features')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')
    options.add_argument('--headless')
    options.add_argument('--verbose')
    return webdriver.Chrome(options=options)


def wait_for_element_text(_browser, xpath):
    return WebDriverWait(_browser, THINK_TIME).until(EC.visibility_of_element_located((By.XPATH, xpath))).text


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
    pq.write_table(table, 'combinations.parquet')


def send_email(_subject, _body):
    email_sender = os.getenv('email_sender')
    email_password = os.getenv('email_password')
    email_receiver = os.getenv('email_receiver')

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = _subject
    em.set_content(_body)

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.send_message(em)
        logging.info('Email sent successfully.')


def contains_all_elements(df, elements_list):
    df_values_set = set(df.values.flatten())
    return all(element in df_values_set for element in elements_list)


def select_random_rows(parquet_file_path, num_rows):
    df = pd.read_parquet(parquet_file_path)
    if num_rows > len(df):
        raise ValueError(f"Requested number of rows ({num_rows}) exceeds the total number of rows ({len(df)}).")
    return df.sample(n=num_rows)


if __name__ == '__main__':
    browser = initialize_browser()
    browser.get(url=URL)

    numbers = [wait_for_element_text(browser, xpath) for xpath in XPATHS]
    logging.info(f"Winning numbers are {numbers}")

    date = wait_for_element_text(browser, DATE_XPATH)

    browser.quit()

    df_random = select_random_rows('combinations.parquet', PLAYS)
    df_random.to_csv('combinations.csv', index=False)

    logging.info(f"\n\n\n"
                 f"{df_random.to_markdown(index=False)}")

    subject = f"Winning numbers of {date}"
    body = (
        f"Winning numbers: {numbers}\n"
        f"Winning the jackpot? {'Yes' if contains_all_elements(df_random, numbers) else 'No'}\n"
        f"Your numbers are:\n{df_random.head(5).to_string(index=False)}"
    )

    # send_email(subject, body)
