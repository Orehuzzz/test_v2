import os
import time
import glob
import pandas as pd
from params.parametrs import username, password, localhost
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, text

DOWNLOAD_DIR = os.path.abspath("tmp")


def clean_tmp_folder():
    '''Удаляем предыдущий файл из папки, чтобы не возникало конфликтов'''
    files = glob.glob(os.path.join(DOWNLOAD_DIR, "data*.csv"))
    for f in files:
        try:
            os.remove(f)
            print(f"Удалён файл: {f}")
        except Exception as e:
            print(f"{f}: не найден")


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def download_csv(driver, url, new_filename='data.csv'):
    '''=Прокладываем путь до csv-файла='''
    wait = WebDriverWait(driver, 15)

    driver.get(url)

    first_button = wait.until(EC.element_to_be_clickable((By.ID, "x-auto-30")))
    first_button.click()
    print("First step")

    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "div.cc-but#x-auto-104 button"))
    )
    download_button.click()

    download_link = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//div[@id='x-auto-142']//a[contains(text(), 'Скачать')]"))
    )
    file_name = download_link.get_attribute("download").split("/")[-1]
    print(f"Устанавливаем: {file_name}")
    download_link.click()

    for i in range(3):
        csv_files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv"))
        if csv_files:
            latest_file = max(csv_files, key=os.path.getctime)

            # Проверка: если целевой файл уже существует — не перезаписываем
            new_path = os.path.join(DOWNLOAD_DIR, new_filename)
            if os.path.exists(new_path):
                # Добавим суффикс времени
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                new_path = os.path.join(DOWNLOAD_DIR, f"data_{timestamp}.csv")

            os.rename(latest_file, new_path)
            print(f"Файл: {new_path} установлен")
            return new_path

        print(f"Ожидание файла... попытка {i + 1}")
        time.sleep(1)

    raise FileNotFoundError("CSV не был найден после тайм-аута")



def process_and_overwrite_csv(file_path):
    '''=Корректируем csv-файл при помощи pandas-а и перезаписываем='''
    regions_dict = {
        30: "Центральный федеральный округ",
        14000000: "Белгородская область",
        15000000: "Брянская область",
        17000000: "Владимирская область",
        20000000: "Воронежская область",
        24000000: "Ивановская область",
        29000000: "Калужская область",
        34000000: "Костромская область",
        38000000: "Курская область",
        42000000: "Липецкая область",
        46000000: "Московская область",
        54000000: "Орловская область",
        61000000: "Рязанская область",
        66000000: "Смоленская область",
        68000000: "Тамбовская область",
        28000000: "Тверская область",
        70000000: "Тульская область",
        78000000: "Ярославская область",
        45000000: "Город Москва",
    }

    indexes_dict = {
        1102: 'К соответствующему периоду прошлого года',
        1104: 'К соответствующему месяцу прошлого года',
        1105: 'В % к предыдущему месяцу'
    }

    df = pd.read_csv(file_path)

    df['region'] = df['region'].map(regions_dict)
    df['id_indicator_name'] = df['id_indicator'].map(indexes_dict)

    final_df = df[['region', 'id_indicator_name', 'value']]
    pivot_df = final_df.pivot(index='region', columns='id_indicator_name', values='value')
    pivot_df = pivot_df.reset_index()
    pivot_df.columns = ['region', 'В % к предыдущему месяцу', 'К соответствующему месяцу прошлого года',
                        'К соответствующему периоду прошлого года']

    central = pivot_df[pivot_df['region'] == 'Центральный федеральный округ']
    moscow = pivot_df[pivot_df['region'] == 'Город Москва']
    others = pivot_df[
        (pivot_df['region'] != 'Центральный федеральный округ') &
        (pivot_df['region'] != 'Город Москва')
        ]

    pivot_df = pd.concat([central, others, moscow], ignore_index=True)

    pivot_df.to_csv(file_path, index=False)
    print(f"Перезаписан файл: {file_path}")
    print(pivot_df.to_string())



def clean_float(x):
    '''=Корректируем данные для вставки в БД='''
    try:
        return float(x.replace(',', '.')) if isinstance(x, str) else float(x)
    except Exception:
        return None


# Загрузка CSV в базу данных PostgreSQL
def load_csv_to_db(csv_path, username, password, localhost, port=5432, db_name='main_database'):

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{localhost}:{port}/{db_name}",
        connect_args={"connect_timeout": 10},
        pool_pre_ping=True
    )

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS pictures_data"))

    metadata = MetaData()

    region_table = Table(
        'regions_data_table', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('region', String(100)),
        Column('index_prev_month', Float),
        Column('index_same_month_last_year', Float),
        Column('index_period_last_year', Float),
        schema='pictures_data'
    )

    metadata.create_all(engine)

    df = pd.read_csv(
        csv_path,
        header=None,
        names=["region", "value1", "value2", "value3"],
        dtype=str,
        encoding='utf-8',
        on_bad_lines='skip'
    )

    for col in ["value1", "value2", "value3"]:
        df[col] = df[col].apply(clean_float)

    df = df.rename(columns={
        "value1": "index_prev_month",
        "value2": "index_same_month_last_year",
        "value3": "index_period_last_year"
    })

    df = df.dropna(subset=["region", "index_prev_month", "index_same_month_last_year", "index_period_last_year"])

    print(f"Будет вставлено строк: {len(df)}")
    print(df.head())

    try:
        with engine.begin() as conn:
            # При необходимости очищаем таблицу и сбрасываем PRIMARY KEY
            conn.execute(text("DELETE FROM pictures_data.regions_data_table"))
            conn.execute(text("ALTER SEQUENCE pictures_data.regions_data_table_id_seq RESTART WITH 1"))

            df.to_sql(
                name='regions_data_table',
                con=conn,
                if_exists='append',
                index=False,
                schema='pictures_data',
                method='multi',
                chunksize=1000
            )
        print("Данные успешно вставлены в БД")
    except Exception as e:
        print(f"Ошибка при вставке в БД: {e}")


def main():

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    clean_tmp_folder()

    try:
        driver = setup_driver()
        url = 'https://bi.gks.ru/biportal/contourbi.jsp?project=%2FDashboard%2Ftrade&report=_1102_%D0%98%D0%BD%D0%B4_%D1%84%D0%B8%D0%B7_%D0%BE%D0%B1%D1%8C%D0%B5%D0%BC%D0%B0_-_%D0%92%D0%B8%D1%82%D0%B0&toolbar=off&slice=slice1&view=view1'
        file_path = download_csv(driver, url, new_filename="data.csv")
        process_and_overwrite_csv(file_path)
        load_csv_to_db(file_path, username, password, localhost)
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()