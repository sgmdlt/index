import aiohttp
import asyncio
import csv
import json
import os
import random
import re
import requests
import urllib.parse

from bs4 import BeautifulSoup
from datetime import datetime

# Заголовки для запроса
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36"
}
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)


# Обработка ошибок при запросе
async def send_request(url, task_number, session):
    # Распределяем по всем прокси
    global PROXIES_LIST
    proxy_config = "http://" + PROXIES_LIST[(task_number - 1) %
                                            len(PROXIES_LIST)]

    # print(url)

    # Задержка между запросами
    await asyncio.sleep(random.uniform(1, 3))

    # Цикл ответа
    got_resp = False
    verify_cert = False
    while not got_resp:
        try:

            # Отправляем запрос без проверки сертификата (прокси при необходимости)
            async with session.get(url, proxy=proxy_config,
                                   ssl=verify_cert) as resp:
                final_resp = await resp.text()
                if resp.ok:
                    got_resp = True
                else:
                    print(f"🤖 {task_number:>2d} |", resp.status, resp.reason)
                    print(url)
                    await asyncio.sleep(5)

        # Обработка ошибок
        except Exception as e:
            # Если проблема в сертификате, отключаем проверку и пробуем снова
            if "certificate has expired" in str(e):
                verify_cert = False
            else:
                print(str(e))
                print(type(e).__name__)
                print(url)
                await asyncio.sleep(5)

    return final_resp


# Главный обработчик списка судов
async def get_cases(task_number, courts, query_list, output_filename):

    # Случайная задержка по времени для разводки запросов
    await asyncio.sleep(random.uniform(1, 10))

    # Проверяем суды по списку
    total_count = 0
    for court in courts:

        # Формируем ссылку
        for query_set in query_list:

            # Запускаем сессию заново для каждого запроса
            async with aiohttp.ClientSession(headers=HEADERS) as session:

                url = "https://mos-gorsud.ru/search?courtAlias=" + court + "&caseDateFrom=" + query_set[
                    "entry_date_from"] + "&caseDateTo=" + query_set[
                        "entry_date_to"] + "&instance=" + query_set[
                            "instance"] + "&processType=" + query_set[
                                "type"] + "&codex=" + urllib.parse.quote_plus(
                                    query_set["article"], encoding="utf-8"
                                ) + "&category=" + query_set["mat_category"]

                # Цикл для нескольких страниц
                next_page = True
                card_count = 0
                target_count = 0
                while next_page:

                    # Отправляем запрос
                    response = await send_request(url, task_number, session)
                    soup = BeautifulSoup(response.replace("</br>", "<br>"),
                                         "html.parser")

                    # Один раз записываем, сколько надо скачать
                    if target_count == 0:
                        if soup.select("div.resultsearch_text"):
                            if target_str := re.search(
                                    r"По вашему запросу найдено записей: (\d+)",
                                    soup.select_one(
                                        "div.resultsearch_text").get_text()):
                                target_count = int(target_str.group(1))

                    # Перебор рядов
                    for row in soup.select(
                            "div.wrapper-search-tables > table > tbody tr"):

                        # Записываем в файл
                        if row.select("td")[0].select_one("a.detailsLink"):
                            link = row.select("td")[0].select_one(
                                "a.detailsLink")["href"]
                            link = "https://mos-gorsud.ru" + link[:link.
                                                                  find("?")]
                            open(output_filename + ".txt",
                                 "a").write(link + "\n")

                            card_count += 1
                            total_count += 1

                    # Перебор страниц
                    next_page = False
                    for link in soup.select(
                            "div.paginationContainer a.next[href]"):
                        url = "https://mos-gorsud.ru" + link["href"]
                        next_page = True
                        break

                    # Отчитываемся через каждые 500 карточек
                    if card_count % 500 == 0 and target_count > 0:
                        print(
                            f"🤖 {task_number:>2d} | {COURT_COLLECTION[court]:<40s} 📇 {card_count / target_count:>4.0%} карточек"
                        )

                print(f"🤖 {task_number:>2d}",
                      f"{COURT_COLLECTION[court]:<40s}",
                      query_set["entry_date_from"],
                      query_set["entry_date_to"],
                      query_set["instance"],
                      TYPE_COLLECTION[query_set["type"]],
                      end=" | ",
                      sep=" | ")

                # Проверка результата
                if card_count > 0 and card_count >= target_count:
                    print(f"🏁 {card_count:>4d}")

                # Если ничего не нашли, ставим ноль
                elif soup.select(
                        "div.resultsearch_text:-soup-contains('По вашему запросу ничего не найдено')"
                ):
                    print("🫙")

                # Если что-то незнакомое
                else:
                    print("🟥🟥🟥🟥")
                    print(url)

    print(
        f"🤖 {task_number:>2d} | {'🟢 ЗАКОНЧИЛ':>30s}: {total_count:>4d} карточек"
    )
    return total_count


# Удаление дубликатов из файла
def remove_duplicates(filename):

    print("\nУбираем дубликаты в:", filename)

    unique_ids = set()
    count = 0
    with open(filename + ".txt") as input_file:
        for line in input_file:
            if line not in unique_ids:
                open(filename + ".txt" + "c", "a").write(line)
                unique_ids.add(line)
                count += 1

        input_file.close()

    os.replace(filename + ".txt" + "c", filename + ".txt")

    print("Без дубликатов записано:", count)


# Запуск параллельной обработки судов
async def court_runner(output_filename, court_codes, query_set, tasks):

    # Загружаем список судов
    print("В списке", len(court_codes), "судов")

    # Создаем перечень процессов
    coro_list = []
    batch_size = round(len(court_codes) / tasks)
    print("Судов на один процесс:", batch_size, "\n\n")
    for i in range(tasks):
        if i < tasks - 1:
            courts_batch = court_codes[i * batch_size:i * batch_size +
                                       batch_size]
        else:
            courts_batch = court_codes[i * batch_size:]
        coro_list.append(
            get_cases(i + 1, courts_batch, query_set, output_filename))

    # Запускаем все процессы
    gather_start = datetime.now()
    gather_result = await asyncio.gather(*coro_list)

    # После окончания собираем результаты в общий список
    final_count = 0
    for total_count in gather_result:
        final_count += total_count

    print("\n\nЗаписано в файл результатов:", final_count)

    # Удаление дубликатов
    remove_duplicates(output_filename)

    print("Прошло", int(
        (datetime.now() - gather_start).total_seconds() / 3600), "час(ов)")


# ГЛАВНЫЙ ЗАПУСК

# Загружаем прокси
PROXIES_LIST = open("proxy.txt").read().splitlines()

# Параметры запроса
QUERY_SET = {
    "entry_date_from": "",
    "entry_date_to": "",
    "instance": "",
    "type": "",
    "article": "",
    "mat_category": ""
}

COURT_COLLECTION = {
    "mgs": "Московский городской суд",
    "babushkinskij": "Бабушкинский районный суд",
    "basmannyj": "Басманный районный суд",
    "butyrskij": "Бутырский районный суд",
    "gagarinskij": "Гагаринский районный суд",
    "golovinskij": "Головинский районный суд",
    "dorogomilovskij": "Дорогомиловский районный суд",
    "zamoskvoreckij": "Замоскворецкий районный суд",
    "zelenogradskij": "Зеленоградский районный суд",
    "zyuzinskij": "Зюзинский районный суд",
    "izmajlovskij": "Измайловский районный суд",
    "koptevskij": "Коптевский районный суд",
    "kuzminskij": "Кузьминский районный суд",
    "kuncevskij": "Кунцевский районный суд",
    "lefortovskij": "Лефортовский районный суд",
    "lyublinskij": "Люблинский районный суд",
    "meshchanskij": "Мещанский районный суд",
    "nagatinskij": "Нагатинский районный суд",
    "nikulinskij": "Никулинский районный суд",
    "ostankinskij": "Останкинский районный суд",
    "perovskij": "Перовский районный суд",
    "preobrazhenskij": "Преображенский районный суд",
    "presnenskij": "Пресненский районный суд",
    "savyolovskij": "Савёловский районный суд",
    "simonovskij": "Симоновский районный суд",
    "solncevskij": "Солнцевский районный суд",
    "taganskij": "Таганский районный суд",
    "tverskoj": "Тверской районный суд",
    "timiryazevskij": "Тимирязевский районный суд",
    "troickij": "Троицкий районный суд",
    "tushinskij": "Тушинский районный суд",
    "hamovnicheskij": "Хамовнический районный суд",
    "horoshevskij": "Хорошёвский районный суд",
    "cheryomushkinskij": "Черёмушкинский районный суд",
    "chertanovskij": "Чертановский районный суд",
    "shcherbinskij": "Щербинский районный суд"
}

TYPE_COLLECTION = {
    # "1": "КАС",
    "2": "ГК"
    # ,
    # "3": "КоАП",
    # "4": "Перв",
    # "5": "Мат",
    # "6": "УК"
}

# Набор последовательных запросов с разбиением по месяца
query_list = []
# QUERY_SET["entry_date_from"] = ""
# QUERY_SET["entry_date_to"] = "01.01.2010"
# query_list.append(QUERY_SET.copy())
current_year = datetime.now().year
current_month = datetime.now().month
for instance in [1]:
    QUERY_SET["instance"] = str(instance)
    # for case_type in list(TYPE_COLLECTION.keys()):
    for case_type in list(TYPE_COLLECTION.keys()):
        QUERY_SET["type"] = case_type

        # QUERY_SET["article"] = "Ст. 20.3,"
        QUERY_SET["entry_date_from"] = "01.01.2025"
        QUERY_SET["mat_category"] = "f033ef9"
        query_list.append(QUERY_SET.copy())

        # current_year = datetime.now().year
        # current_month = datetime.now().month
        # for year in range(2024, 2025):
        #     for month in range(1, 12):
        #         QUERY_SET["entry_date_from"] = f"01.{month:02d}.{year}"
        #         QUERY_SET["entry_date_to"] = f"01.{month + 1:02d}.{year}"
        #         query_list.append(QUERY_SET.copy())
        #     QUERY_SET["entry_date_from"] = f"01.12.{year}"
        #     QUERY_SET["entry_date_to"] = f"01.01.{year + 1}"
        #     query_list.append(QUERY_SET.copy())

        # for month in range(1, current_month):
        #     QUERY_SET["entry_date_from"] = f"01.{month:02d}.{current_year}"
        #     QUERY_SET["entry_date_to"] = f"01.{month + 1:02d}.{current_year}"
        #     query_list.append(QUERY_SET.copy())
        # QUERY_SET["entry_date_from"] = f"01.{current_month}.{current_year}"
        # QUERY_SET["entry_date_to"] = ""
        # query_list.append(QUERY_SET.copy())

# Запуск сбора
# Имя файла для результата, набор судов, список (!) поисковых запросов, количество процессов
asyncio.run(
    court_runner("MGS-Missing-Update-Links", list(COURT_COLLECTION.keys()),
                 query_list, 36))