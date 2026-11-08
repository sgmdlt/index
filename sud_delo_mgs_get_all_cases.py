import aiohttp
import asyncio
import docx2txt
import gc
import json
import pdfplumber
import random
import re
import subprocess
import sys
import tempfile

from striprtf.striprtf import rtf_to_text

from bs4 import BeautifulSoup
from datetime import datetime

# Заголовки для запроса
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
}
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)


# Обработка ошибок при запросе
async def send_request(url, task_number, session):
    # Распределяем по всем прокси
    global PROXIES_LIST
    proxy_config = "http://" + PROXIES_LIST[task_number % len(PROXIES_LIST)]

    # print(url)

    # Задержка между запросами
    await asyncio.sleep(random.uniform(1, 3))

    # Цикл ответа
    got_resp = False
    verify_cert = False
    while not got_resp:
        try:

            # Отправляем запрос без проверки сертификата (прокси при необходимости)
            async with session.get(url, ssl=verify_cert,
                                   proxy=proxy_config) as resp:
                final_resp = await resp.text()
                if resp.ok:
                    got_resp = True
                else:
                    final_resp = "ОШИБКА"
                    # print(resp.status, resp.reason)
                    got_resp = True

        # Обработка ошибок
        except Exception as e:
            # print(proxy_config, final_resp[:10])
            # Если проблема в сертификате, отключаем проверку и пробуем снова
            if "certificate has expired" in str(e):
                verify_cert = False
            elif "Connect call failed" in str(e):
                print("Сбой прокси", proxy_config)
                await asyncio.sleep(10)
            elif "TimeoutError" not in type(e).__name__:
                print(str(e))
                print(type(e).__name__)
                print(url)
                await asyncio.sleep(5)

    # print(proxy_config, final_resp[:10])
    return final_resp


# ТЕКСТЫ РЕШЕНИЙ
async def get_doc_text(url, task_number, session):
    global PROXIES_LIST

    # print(url)

    # Скачиваем содержимое по ссылке байтами
    got_resp = False
    resp_bytes = b""
    resp_ext = ""
    output_string = ""
    attempts = 0
    verify_cert = False
    proxy_config = "http://" + PROXIES_LIST[task_number % len(PROXIES_LIST)]
    while not got_resp:
        try:
            async with session.get(url, proxy=proxy_config,
                                   ssl=verify_cert) as resp:
                resp_bytes = await resp.read()

                # Если файл на месте, записываем расширение
                if resp.status == 200:
                    resp_ext = resp.headers["Content-Disposition"][
                        resp.headers["Content-Disposition"].rfind("."):].lower(
                        ).replace(";", "").replace("\"", "")
                    got_resp = True
                    await asyncio.sleep(2)

                # Если файл не найден, сразу пропускаем
                elif resp.status in [404, 500]:
                    got_resp = True
                    await asyncio.sleep(2)

                # При другой ошибке сервера ждём и пробуем снова
                else:
                    await asyncio.sleep(5)

        # Обработка ошибок
        except Exception as e:
            if isinstance(e, TimeoutError):
                pass
            elif "Connect call failed" in str(e):
                print("Сбой прокси", proxy_config)
                await asyncio.sleep(10)
                attempts = 0
            else:
                print(str(e))
                print(type(e).__name__)
                print(url)
                await asyncio.sleep(5)

        if attempts > 3:
            raise ValueError("Текст недоступен")
        else:
            attempts += 1

    # Сохраняем во временный файл и забираем весь текст
    with tempfile.NamedTemporaryFile(suffix=resp_ext) as temp:
        temp.write(resp_bytes)
        temp.flush()

        # Пробуем все виды расшифровки сразу
        # Сначала пробуем .doc
        p = subprocess.Popen("antiword -m UTF-8.txt -w 0 " + temp.name,
                             stdout=subprocess.PIPE,
                             shell=True)
        stdout, stderr = p.communicate()
        output_string = stdout.decode("utf-8", "ignore")

        # Если он ничего не дал, пробуем .docx
        if not output_string:
            try:
                output_string = docx2txt.process(temp.name)
            except Exception as e:
                pass

        # Если он ничего не дал, пробуем .pdf
        if not output_string:
            try:
                pdf = pdfplumber.open(temp.name)
                for page in pdf.pages:
                    output_string += page.extract_text()
            except Exception as e:
                pass

        # Если он ничего не дал, пробуем RTF
        if not output_string:
            try:
                output_string = rtf_to_text(resp_bytes.decode(),
                                            errors="ignore")
            except Exception as e:
                pass

        # В последнюю очередь пробуем TXT с двумя кодировками
        if not output_string:
            try:
                output_string += resp_bytes.decode("1251", "ignore")
                output_string += resp_bytes.decode("utf-8", "ignore")
            except Exception as e:
                pass

        # Если необычное расширение, сигналим
        if resp_ext not in [
                "", ".doc", ".docx", ".rtf", ".pdf", ".txt", ".png", ".jpg",
                ".jpeg", ".tiff"
        ]:
            print(f"🤖 {task_number:>2d}",
                  "НОВЫЙ ФОРМАТ ДОКУМЕНТА:",
                  resp_ext,
                  sep=" | ")
            print(url)

        temp.close()

    # Если получен текст, сохраняем результат в файл
    result_str = ""
    if output_string and "временно недоступен" not in output_string:

        if resp_ext not in [".png", ".jpg", ".jpeg", ".tiff"
                            ] and "/Image" not in output_string:
            # Очищаем от неразрывных пробелов
            output_string = output_string.replace(u"\u00A0", " ")

            result_str = output_string

    return result_str


# Преобразование таблиц
async def tab_to_dict(soup, tab, task_number, session):
    tab_dict = []
    for row in tab.select("tbody tr"):
        row_dict = {}

        # Если ошибка с ячейками, заполняем пустыми до конца
        tag_diff = len(tab.select("thead th")) - len(row.select("td"))
        if tag_diff > 0:
            for i in range(tag_diff):
                row.append(soup.new_tag("td"))

        for key, value in zip(tab.select("thead th"),
                              row.select("td"),
                              strict=True):
            if file := value.select_one("a:-soup-contains('Скачать файл')"):
                row_dict[key.get_text(" ", strip=True)] = await get_doc_text(
                    "https://mos-gorsud.ru" + file["href"], task_number,
                    session)
            else:
                row_dict[key.get_text(" ",
                                      strip=True)] = value.get_text(" ",
                                                                    strip=True)
        tab_dict.append(row_dict)
    return tab_dict


# Создание идентификатора
def get_custom_id(court_code, date_reg, case_number):
    # Создаем идентификатор
    id_date = "NONE"
    if date_reg:
        id_date = date_reg
    case_id = court_code + "_" + id_date + "_"

    if id_date != "NONE" and re.search(r"[/\\\|] ?" + date_reg[:4],
                                       case_number):
        case_id += re.findall(r"([^ ~∼\(\[\{]+) ?[/\\\|] ?" + date_reg[:4],
                              case_number)[-1]
    elif re.search(r"[/\\\|] ?(?:20|19)", case_number):
        case_id += re.findall(r"([^ ~∼\(\[\{]+) ?[/\\\|] ?(?:20|19)",
                              case_number)[-1]
    else:
        print("Неопознанный номер дела:", case_number)
        case_id += case_number

    return case_id.replace(" ", "").replace("/", "-").replace("\\", "-")


# Главный обработчик списка карточек
async def get_acts(task_number, queue, input_filename):
    global PROXIES_LIST

    # Случайная задержка по времени для разводки запросов
    await asyncio.sleep(random.uniform(1, 10))

    # Запускаем сессию для каждого процесса
    async with aiohttp.ClientSession(
        headers=HEADERS, timeout=aiohttp.ClientTimeout(total=60)) as session:

        # Проверяем решения по списку
        while True:
            result = await queue.get()

            # Отправляем запрос
            response = await send_request(result.strip(), task_number, session)
            soup = BeautifulSoup(response, "html.parser")

            # Если все в порядке
            if "ОШИБКА" not in response and soup.select_one("div.detail-cart"):

                # Заполняем базовые поля принудительно
                formatted_case = {
                    "region":
                    "Город Москва",
                    "court_name":
                    soup.select("ul.breadcrumb li")[1].get_text(" ",
                                                                strip=True),
                    "date_reg":
                    datetime.strptime(
                        soup.select_one(
                            "div.row_card:-soup-contains('Дата поступления', 'Дата регистрации') div.right"
                        ).get_text(" ", strip=True),
                        "%d.%m.%Y").strftime("%Y-%m-%d"),
                    "court_code":
                    soup.select_one("input[name=courtCode]")["value"],
                    "link":
                    result.strip()
                }

                # Заполняем тип дела и инстанцию
                case_type = formatted_case[
                    "link"][formatted_case["link"].find("cases") +
                            6:formatted_case["link"].find("details") - 1]
                match case_type:
                    case "criminal" | "claim-criminal" | "first-criminal":
                        formatted_case["case_type"] = "Уголовное дело"
                        formatted_case["instance"] = "Первая инстанция"
                    case "civil" | "claim-civil" | "first-civil":
                        formatted_case["case_type"] = "Гражданское дело"
                        formatted_case["instance"] = "Первая инстанция"
                    case "admin":
                        formatted_case[
                            "case_type"] = "Дело об административном правонарушении"
                        formatted_case["instance"] = "Первая инстанция"
                    case "kas" | "claim-admin" | "first-admin":
                        formatted_case["case_type"] = "Административное дело"
                        formatted_case["instance"] = "Первая инстанция"
                    case "criminal-materials" | "civil-exec" | "admin-exec":
                        formatted_case[
                            "case_type"] = "Производство по материалам"
                        formatted_case["instance"] = "Первая инстанция"
                    case "first-criminal-exec" | "first-civil-exec" | "first-admin-exec":
                        formatted_case[
                            "case_type"] = "Производство по материалам"
                        formatted_case["instance"] = "Первая инстанция"
                    case "appeal-criminal":
                        formatted_case["case_type"] = "Уголовное дело"
                        formatted_case["instance"] = "Апелляция"
                    case "appeal-civil":
                        formatted_case["case_type"] = "Гражданское дело"
                        formatted_case["instance"] = "Апелляция"
                    case "appeal-admin" | "review-not-yet":
                        formatted_case[
                            "case_type"] = "Дело об административном правонарушении"
                        formatted_case["instance"] = "Апелляция"
                    case "appeal-kas":
                        formatted_case["case_type"] = "Административное дело"
                        formatted_case["instance"] = "Апелляция"
                    case _:
                        print("Неизвестный тип:", case_type)

                #Исключение для Мосгорсуда
                if "mgs/services/cases/appeal-admin" in formatted_case["link"]:
                    formatted_case["case_type"] = "Административное дело"
                    formatted_case["instance"] = "Апелляция"

                # Составляем идентификатор
                case_number = soup.select_one(
                    "h1:-soup-contains('Информация')").get_text(" ",
                                                                strip=True)
                formatted_case["case_number"] = case_number[case_number.
                                                            find("№") + 2:]
                formatted_case["id_final"] = get_custom_id(
                    formatted_case["court_code"], formatted_case["date_reg"],
                    formatted_case["case_number"])

                # ТАБЛИЦЫ > JSON
                formatted_case["raw_tables"] = {}

                try:

                    # Отдельно заглавная таблица
                    formatted_case["raw_tables"]["Дело"] = {}
                    for row_card in soup.select(
                            "div.detail-cart div.row_card"):
                        formatted_case["raw_tables"]["Дело"][
                            row_card.select_one("div.left").get_text(
                                " ", strip=True)] = row_card.select_one(
                                    "div.right").get_text(" ", strip=True)

                        # Если есть ссылка, дописываем её туда же
                        if row_card_link := row_card.select_one("div.right a"):
                            formatted_case["raw_tables"]["Дело"][
                                row_card.select_one("div.left").get_text(
                                    " ", strip=True
                                )] += "\n\nhttps://mos-sud.ru" + row_card_link[
                                    "href"]

                    # Берём вкладки
                    for tab_name, tab in zip(
                            soup.select("div.tabs_wrapper ul li"),
                            soup.select("div.tabs_wrapper div.tab-pane"),
                            strict=True):
                        tab_name = tab_name.get_text(" ", strip=True)
                        formatted_case["raw_tables"][tab_name] = []

                        # Отдельно для видеозаписей
                        if "detail-session-video" in str(tab):
                            for cardsud, iframe in zip(
                                    tab.select("div.cardsud_wrapper"),
                                    tab.select("iframe"),
                                    strict=True):
                                session_video = {}
                                for row_card in cardsud.select("div.row_card"):
                                    session_video[row_card.select_one(
                                        "div.left").get_text(
                                            " ",
                                            strip=True)] = row_card.select_one(
                                                "div.right").get_text(
                                                    " ", strip=True)
                                session_video["embed_link"] = iframe["src"]
                                formatted_case["raw_tables"][tab_name].append(
                                    session_video)

                        # Берём таблицы
                        elif "h3" in str(tab):
                            table_names = []
                            # Если таблиц на одну больше, чем заголовков
                            if len(tab.select("table")) > len(
                                    tab.select("h3")):
                                if len(tab.select("table")) == len(
                                        tab.select("h3")) + 1:
                                    table_names.append(tab_name)
                                else:
                                    print("Слишком мало заголовков")
                                    print(
                                        json.dumps(formatted_case,
                                                   ensure_ascii=False))
                                    raise ValueError("Слишком мало заголовков")
                            for tab_header in tab.select("h3"):
                                table_names.append(
                                    tab_header.get_text(" ", strip=True))

                            # Сохраняем таблицы под заголовками
                            for table_name, table in zip(table_names,
                                                         tab.select("table"),
                                                         strict=True):
                                formatted_case["raw_tables"][tab_name].append({
                                    table_name:
                                    await tab_to_dict(soup, table, task_number,
                                                      session)
                                })
                        # Когда одна таблица
                        else:
                            formatted_case["raw_tables"][
                                tab_name] = await tab_to_dict(
                                    soup, tab.select_one("table"), task_number,
                                    session)

                    # Заменяем номер и формируем более точный идентификатор
                    number_found = False
                    for key in formatted_case["raw_tables"]["Дело"].keys():
                        if key.startswith(
                            ("Номер дела", "Номер жалобы", "Номер заявления",
                             "Номер материала", "Номер производства",
                             "Номер дела ~ материала"
                             )) and "инстанц" not in key:
                            number_found = True
                            formatted_case["case_number"] = " ".join(
                                formatted_case["raw_tables"]["Дело"]
                                [key].replace("\n", "").split())
                            formatted_case["id_final"] = get_custom_id(
                                formatted_case["court_code"],
                                formatted_case["date_reg"],
                                formatted_case["case_number"])

                    if not number_found:
                        print("Необычный номер дела")
                        print(json.dumps(formatted_case, ensure_ascii=False))
                        raise ValueError("Необычный номер дела")

                    # Записываем в файл по коду суда
                    # print(json.dumps(formatted_case, ensure_ascii=False))

                    # file_path = "RS-KoAP20.3.jsonl"
                    file_path = f"cases_by_court/{sys.argv[2]}/" + formatted_case[
                        "court_code"][2:4] + "/" + formatted_case[
                            "court_code"] + ".jsonl"
                    open(file_path, "a").write(
                        json.dumps(formatted_case, ensure_ascii=False).replace(
                            u"\u00A0", " ") + "\n")

                # Ошибки обработки откладываем на потом
                except Exception as e:
                    # print(str(e))
                    open(input_filename + "-Cases-Error.txt",
                         "a").write(result.strip() + "\n")

            # Сигналим о выполнении задачи
            queue.task_done()

            # Отчитываемся каждые 100 решений
            if queue._unfinished_tasks % 100 == 0:
                print(
                    "Пройдено из партии", "{:>4.0%}".format(
                        (10000 - queue._unfinished_tasks) / 10000))


# Запуск параллельной обработки решений
async def acts_runner(input_filename, task_count, last_count):
    print("Список дел", input_filename)
    print("Начинаем со строчки", f"{last_count:,}".replace(",", " "))

    # Загружаем список результатов
    queue = asyncio.Queue()
    case_count = 0
    batch_count = 0
    gather_start = datetime.now()
    with open(input_filename + ".txt") as input_file:
        for line in input_file:
            case_count += 1

            # Отступаем нужное количество номеров
            if case_count < last_count:
                pass
            else:

                # Заполняем пачку 10к карточек
                if batch_count < 10000:
                    batch_count += 1
                    queue.put_nowait(line)
                else:

                    print("Запускаем новую партию")

                    # Создаём задачи
                    tasks = []
                    for i in range(task_count):
                        task = asyncio.create_task(
                            get_acts(i + 1, queue, input_filename))
                        tasks.append(task)

                    # Запускаем очередь
                    await queue.join()

                    # Завершаем задачи
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Очищаем память
                    del queue
                    gc.collect()

                    print("Последняя пройденная строчка",
                          f"{case_count:,}".replace(",", " "))

                    # Обнуляем список, записывая текущую строчку
                    queue = asyncio.Queue()
                    batch_count = 1
                    queue.put_nowait(line)

    # Последняя пачка
    if batch_count > 0:
        print("Загружена последняя партия")

        # Создаём задачи
        tasks = []
        for i in range(task_count):
            task = asyncio.create_task(get_acts(i + 1, queue, input_filename))
            tasks.append(task)

        # Запускаем очередь
        await queue.join()

        # Завершаем задачи
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Очищаем память
        del queue
        gc.collect()

        print("Последняя строчка", f"{case_count:,}".replace(",", " "),
              "пройдена, прошло",
              int((datetime.now() - gather_start).total_seconds() / 3600),
              "час(ов)")


# ГЛАВНЫЙ ЗАПУСК

# Загружаем прокси
PROXIES_LIST = open("proxy.txt").read().splitlines()

# Для тестов
# sys.argv.append("MGS-KoAP20.3-Links")

# Запуск сбора
# Имя файла с результатами, количество процессов, номер последней пройденной партии (0)
asyncio.run(acts_runner(sys.argv[1], 5, 0))