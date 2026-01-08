import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров из магазина Ozon.

    Args:
    last_id (str): Последний артикул для отбора.
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    list: Список товаров из магазина.

    Examples:
    >>> get_product_list('', '1234', 'Ab123CDe45')
    [...]

    >>> get_product_list(10, 1234)
    TypeError: price_conversion() missing 1 required
        positional argument: 'seller_token'
        Если все таки указать 3й аргумент, тогда получите
        ошибку в ответе от API Ozon.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы товаров из магазина Ozon.

    Args:
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    list: Список артикулов товаров продающихся в магазине.

    Examples:
    >>> get_offer_ids('1234', 'Ab123CDe45')
    [...]

    >>> get_offer_ids(10, 1234)
        Вернется ошибка от API Ozon.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены на товары в магазине Ozon.

    Args:
    prices(:obj:'list' of :obj:'dict'): Информация о цене
        по каждому товару.
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    dict: Cловарь с ответом от API Ozon.

    Examples:
    >>> update_price(prices, '1234', 'Ab123CDe45')
        От API Ozon вернется словарь, сообщающий об успешной загрузке прайсов.

    >>> update_price(10, '1234', 'Ab123CDe45')
        От API Ozon вернется словарь, сообщающий
        об ошибке при загрузке прайсов.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет информацию по остаткам часов на Ozon.

    Args:
    stocks(:obj:'list' of :obj:'dict'): Информация об остатках часов
        продающихся на Ozon.
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    dict: Cловарь с ответом от API Ozon.

    Examples:
    >>> update_price(stocks, '1234', 'Ab123CDe45')
        От API Ozon вернется словарь, сообщающий об
            успешном обновлении остатков.

    >>> update_price(10, '1234', 'Ab123CDe45')
        От API Ozon вернется словарь, сообщающий
        об ошибке при обновлении остатков
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Сохраняет данные по остаткам с сайта ООО "Группа АВГУСТ".

    Функция сначала запрашивает zip файл содержащий данные об остатках
    с сайта часовой компании ООО "Группа АВГУСТ", после чего сохраняет из
    него excel файл. Далее из данных excel таблицы формируется словарь,
    после чего excel файл удаляется.

    Returns:
    dict: Словарь с данными о часах, продающихся на сайте.

    Examples:
    >>> download_stock()
    {...}

    >>> download_stock(10)
    TypeError: price_conversion() takes 0 positional arguments but 1 was given
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")

    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")

    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список с остатками по часам продающимся в магазине.

    Сначала функция формирует остатки для всех товаров, которые
    продаются компанией ООО "Группа АВГУСТ" и в магазине Ozon,
    после этого удаляя артикул из общего списка артикулов offer_ids.
    Далее по оставшимся артикулам в списке offer_ids, которые
    не содержатся в списке часов с сайта ООО "Группа АВГУСТ",
    формируются нулевые остатки.

    Args:
    watch_remnants (dict): Cловарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    offer_ids (list): Список артикулов часов продающихся на Ozon.

    Returns:
    :obj:'list' of :obj:'dict': Список словарей содержащих артикула
        товаров и их остатки на складе.

    Examples:
    >>> create_stocks(watch_remnants, offer_ids)
    [{...},{...},...]

    >>> create_stocks('часы', 10)
    AttributeError: 'str' object has no attribute 'get'
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))

    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует цены для часов, которые продаются в магазине Ozon.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    offer_ids (list): Список артикулов часов продающихся на Ozon.

    Returns:
    :obj:'list' of :obj:'dict': Список словарей содержащих информацию
        об актуальных ценах часов продающихся в магазине Ozon.

    Examples:
    >>> create_prices(watch_remnants, offer_ids)
    [{...},{...},...]

    >>> create_prices('часы', 10)
    AttributeError: 'str' object has no attribute 'get'
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в строку цифр без копеек и лишних символов.

    Args:
    price: Цена, которую необходимо преобразовать.

    Returns:
    str: Преобразованная цена в виде строки.

    Examples:
    >>> price_conversion("5'990.00 руб.")
    '5990'

    >>> price_conversion(5990.00)
    AttributeError: 'float' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список lst по n элементов.

    Args:
    lst: Любой список.
    n: Число элементов, которое необходимо взять из списка за одну итерацию.

    Examples:
    >>> next(divide([1, 2, 3, 4, 5], 2)
    [1, 2]

    >>> update_price([], 2)
    StopIteration

    >>> update_price(1, 2)
    TypeError: object of type 'int' has no len()
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет цены на товары продающиеся на Ozon.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    :obj:'list' of :obj:'dict': Возвращает прайсы, которые были подгружены
        на сайт Ozon.

    Examples:
    >>> create_prices(watch_remnants, client_id, seller_token)
    [{...},{...},...]

    >>> create_prices('часы', '1234', 'Ab123CDe45')
    AttributeError: 'str' object has no attribute 'get'
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров продающихся на Ozon.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    client_id (str): ID магазина.
    seller_token (str): API токен продавца.

    Returns:
    :obj:'list' of :obj:'dict': Список с информацией об остатках товаров
        с ненулевыми остатками на складе.
    :obj:'list' of :obj:'dict': Список с информацией об остатках
        всех товаров на складе.

    Examples:
    >>> upload_stocks(watch_remnants, client_id, seller_token)
    [{...},{...},...], [{...},{...},...]

    >>> upload_stocks('часы', '1234', 'Ab123CDe45')
    AttributeError: 'str' object has no attribute 'get'
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()

        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)

        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)

    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
