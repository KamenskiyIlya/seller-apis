import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из магазина на Яндекс маркете.

    Args:
    page (str): Токен страницы с товарами.
    campaign_id (str): ID магазина.
    access_token (str): API токен продавца.

    Returns:
    list: Список товаров из магазина.

    Examples:
    >>> get_product_list('1', '1234', 'Ab123CDe45')
    [...]

    >>> get_product_list(10, 1234)
    TypeError: price_conversion() missing 1 required
        positional argument: 'access_token'
    """    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет информацию по остаткам часов на Ozon.

    Args:
    stocks(:obj:'list' of :obj:'dict'): Информация об остатках часов
        продающихся на Яндекс маркете.
    campaign_id (str): ID магазина.
    access_token (str): API токен продавца.

    Returns:
    dict: Cловарь с ответом от Яндекс маркета.

    Examples:
    >>> update_price(stocks, '1234', 'Ab123CDe45')
        От API Яндекс маркета вернется словарь, сообщающий об
        успешном обновлении остатков.

    >>> update_price(10, '1234', 'Ab123CDe45')
        От API Яндекс маркета вернется словарь, сообщающий
        об ошибке при обновлении остатков
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены на товары в магазине Яндекс маркета.

    Args:
    prices(:obj:'list' of :obj:'dict'): Информация о цене
        по каждому товару.
    campaign_id (str): ID магазина.
    access_token (str): API токен продавца.

    Returns:
    dict: Cловарь с ответом от API Яндекс маркета.

    Examples:
    >>> update_price(prices, '1234', 'Ab123CDe45')
        От API Яндекс маркета вернется словарь, сообщающий
        об успешной загрузке прайсов.

    >>> update_price(10, '1234', 'Ab123CDe45')
        От API Яндекс маркета вернется словарь, сообщающий
        об ошибке при загрузке прайсов.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров из магазина Яндекс маркета.

    Args:
    campaign_id (str): ID магазина.
    market_token (str): API токен продавца.

    Returns:
    list: Список артикулов товаров продающихся в магазине.

    Examples:
    >>> get_offer_ids('1234', 'Ab123CDe45')
    [...]

    >>> get_offer_ids(10, 1234)
        Вернется ошибка от API Ozon.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует список с остатками по часам продающимся в магазине.

    Сначала функция формирует остатки для всех товаров, которые
    продаются компанией ООО "Группа АВГУСТ" и в магазине Яндекс маркета,
    после этого удаляя артикул из общего списка артикулов offer_ids.
    Далее по оставшимся артикулам в списке offer_ids, которые
    не содержатся в списке часов с сайта ООО "Группа АВГУСТ",
    формируются нулевые остатки.

    Args:
    watch_remnants (dict): Cловарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    offer_ids (list): Список артикулов часов продающихся на Ozon.
    warehouse_id (str): ID склада, на котором хранится товар.

    Returns:
    :obj:'list' of :obj:'dict': Список словарей содержащих артикула
        товаров и их остатки на складе.

    Examples:
    >>> create_stocks(watch_remnants, offer_ids)
    [{...},{...},...]

    >>> create_stocks('часы', 10, 10)
    AttributeError: 'str' object has no attribute 'get'
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует цены для часов, которые продаются в магазине Ozon.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    offer_ids (list): Список артикулов часов продающихся на Ozon.

    Returns:
    :obj:'list' of :obj:'dict': Список словарей содержащих информацию
        об актуальных ценах часов продающихся в магазине Яндекс маркета.

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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены на товары продающиеся на Яндекс маркете.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    campaign_id (str): ID магазина.
    market_token (str): API токен продавца.

    Returns:
    :obj:'list' of :obj:'dict': Возвращает прайсы, которые были подгружены
        на сайт Яндекс маркета.

    Examples:
    >>> create_prices(watch_remnants, client_id, seller_token)
    [{...},{...},...]

    >>> create_prices('часы', '1234', 'Ab123CDe45')
    AttributeError: 'str' object has no attribute 'get'
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет остатки товаров продающихся на Яндекс маркете.

    Args:
    watch_remnants (dict): Словарь с данными о часах,
        продающихся на сайте компании ООО "Группа АВГУСТ".
    campaign_id (str): ID магазина.
    market_token (str): API токен продавца.
    warehouse_id (str): ID склада, на котором хранится товар.

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
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
