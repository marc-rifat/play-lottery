import os
import smtplib
import ssl
from email.message import EmailMessage

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from pick_numbers import select_random_rows

THINK_TIME = 15
PLAYS = 100
URL = 'https://www.valottery.com/data/draw-games/megamillions'

DATE_XPATH = "//h3[@class='title-display']"

XPATHS = [
    "//li[@class='dark-blue-text balls-6'][normalize-space()='28']",
    "//li[@class='dark-blue-text balls-6'][normalize-space()='30']",
    "//li[@class='dark-blue-text balls-6'][normalize-space()='44']",
    "//li[@class='dark-blue-text balls-6'][normalize-space()='66']",
    "//li[@class='dark-blue-text balls-6'][normalize-space()='69']",
    "//span[@id='bonus-ball-display']"
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


def wait_for_element_text(browser, xpath):
    return WebDriverWait(browser, THINK_TIME).until(EC.visibility_of_element_located((By.XPATH, xpath))).text


def send_email(subject, body):
    email_sender = os.getenv('email_sender')
    email_password = os.getenv('email_password')
    email_receiver = os.getenv('email_receiver')

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.send_message(em)


def contains_all_elements(df, elements_list):
    df_values_set = set(df.values.flatten())
    return all(element in df_values_set for element in elements_list)


if __name__ == '__main__':
    browser = initialize_browser()
    browser.get(url=URL)

    numbers = [wait_for_element_text(browser, xpath) for xpath in XPATHS]
    date = wait_for_element_text(browser, DATE_XPATH)

    df_random = select_random_rows('megamillions/files/combinations.parquet', PLAYS)
    df_random.to_csv('megamillions/files/combinations.csv', index=False)

    subject = f"Winning numbers of {date}"
    body = (
        f"Winning numbers: {numbers}\n"
        f"Winning the jackpot? {'Yes' if contains_all_elements(df_random, numbers) else 'No'}\n"
        f"Your numbers are:\n{df_random.head(5).to_string(index=False)}"
    )
    print(body)
    send_email(subject, body)

    browser.quit()
