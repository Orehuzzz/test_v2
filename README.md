# test_v2
#  Моё тестовое задание

##  Описание проекта

Из динамического сайта BI росстата необходимо вытащить данные за определённый период и запулить в БД.

Т.к сайт динамический -> есть несколько подходов для реализации:
1) Мы прописываем весь путь до необходимых данных -> вытаскиваем -> преобразуем -> закидывем в БД.
2) Мы закладываем ссылку на необходимые данные в скрипт -> повторяем все действия из п.1. Проблема: есть веротяность, что ссылка перестанет быть активной.
3) Мы возлагаем ответственность за работу ссылки на пользователя -> повторяем п. 1

Данные перекидываются в БД Postgresql

##  Навигация по проекту

```
Папка scripts_versions:

* start_scripts_all.py - проходится по сайту (местами находит необходимые кнопки по координатам) - 
посл чего получает актуальный url, переходит на него, скачивает csv-файл, преобразует его и
пуляет в БД.
ПРИМЕЧАНИЕ: во всех скриптах time.sleep используется 1 раз для ожидания отзыва csv - файла,
однако в script_all.py - я не нашёл альтернативы для ожидания при нажатии кнопок по координатам.

*script_v1.py - уже имеет заданый url(может быть нестабилным) -> после чего делает все те же дейтсвия, что и
start_script_all.py
 
*script_v2.py - делает то же, что и script_v2, только возлагает ответственность за актуальность ссылки на юзера.
```
```
Папка tmp:
Хранит в себе конечный (преобразованный) файл data.csv. В нашем случае data.csv выгляди следующим образом:
```
![img_1.png](img_1.png)
```
Параметры для передачи данных в БД находятся в
engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{localhost}:{port}/{db_name}",
        connect_args={"connect_timeout": 10},
        pool_pre_ping=True
    )
-> при отрабатывании скриптов данный фрагмент кода не будет отрабатывать, 
если не передадите свои параметры
Данные в БД:
```
![img_2.png](img_2.png)
## Как проверить данные в БД?

```
Хост: 171.22.117.31
```
```
Порт: 5432
```
```
База данных: main_database
```
```
Пользователь: guest
```
```
Пароль: 1ExgPksOwn4K
```
```
Даные лежат на схеме pictures_data в таблице regions_data_table

В самой таблице находятся столбцы: 
id - PRIMARY KEY,
region - информация о названиях регионов, формат String
index_prev_month - индекс физического объёма оборота розничной тарговли в % к соответствующему месяцу 
прошлого года. Формат Float,
index_same_month_last_year - индекс физического объёма оборота розничной тарговли в % к предыдущему месяцу. Формат Float
index_period_last_year - индекс физического объёма оборота розничной тарговли в % к 
соответствующему периоду прошлого года. Формат Float

Для проверки:
SELECT *
FROM pictures_data.regions_data_table;
```
