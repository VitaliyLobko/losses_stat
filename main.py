import requests, re, configparser, pathlib
from datetime import datetime
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.server_api import ServerApi

file_config = pathlib.Path().joinpath('config.ini')
config = configparser.ConfigParser()
config.read(file_config)

username = config.get('DB', 'USER')
password = config.get('DB', 'PASSWORD')
database_name = config.get('DB', 'DB_NAME')
domain = config.get('DB', 'DOMAIN')

url = f"mongodb+srv://{username}:{password}@{domain}/?retryWrites=true&w=majority"
client = MongoClient(url, server_api=ServerApi('1'))
db = client.get_database(database_name)

BASE_URL = "https://index.minfin.com.ua/ua/russian-invading/casualties/"


def get_url():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    content = soup.select("div[class=ajaxmonth] h4[class=normal] a")
    urls = ["/"]
    prefix = "/month.php?month="
    for tag_a in content:
        urls.append(prefix + re.search(r"\d{4}-\d{2}", tag_a["href"]).group())
    return urls


def spider(urls):
    data = []
    for url in urls:
        response = requests.get(BASE_URL + url)
        soup = BeautifulSoup(response.text, "html.parser")
        content = soup.select("ul[class=see-also] li[class=gold]")
        for element in content:
            result = {}
            date = element.find("span", attrs={"class": "black"}).text
            try:
                date = datetime.strptime(date, "%d.%m.%Y").isoformat()
            except ValueError:
                continue

            result.update({"date": date})
            losses = element.find("div").find("div").find("ul")
            for l in losses:
                title, quantity, *rest = l.text.split("—")
                title = title.strip()
                quantity = re.search(r"\d+", quantity).group()
                result.update({title: quantity})
        data.append(result)

    return data


def create_stats(data):
    result = db.data.insert_many(data)


if __name__ == "__main__":
    urls_for_parser = get_url()
    r = spider(urls_for_parser)
    if len(r) > 0:
        create_stats(r)
