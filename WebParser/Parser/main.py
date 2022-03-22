import time
import re
import json
from pprint import pprint

import requests
from bs4 import BeautifulSoup


def change_link(link):
    '''
    Change link to next
    :param link: link to change
    :return: new link
    '''
    result = re.findall(r'\d+', link)
    i = int(result[0]) + 1
    link = re.sub(r'\d+', str(i),link)
    return link


def get_page_links(URL):
    """
    Get links from one page
    :param URL: url
    :return: links from page
    """
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    articles = soup.find_all("div", class_="art")
    links = []
    for article in articles:
        posts = article.find_all("a", class_="art-link")
        for post in posts:
            links.append(post.get('href'))
    print("Page links done")
    return links

def get_all_links(URL, cnt):
    """
    Get all links from pages
    :param URL: url
    :param cnt: pages count
    :return: list of links
    """
    links = []
    for i in range(cnt):
        links = links + get_page_links(URL)
        URL = change_link(URL)
    print("All links done")
    return links


def parse_page(URL):
    """
    Parse one page from url
    :param URL: url
    :return: dic. with required data
    """
    try:
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
    except requests.exceptions.TooManyRedirects:
        pass

    #zahlvi
    try:
        h1 = soup.find_all("h1")
        h1 = h1[0].text
    except IndexError:
        h1 = ''
    #datum
    try:
        date = soup.find_all("span", class_="time-date")
        date = date[0].attrs['content']
        date = re.match('\d+-\d+-\d+',date).group()
    except (IndexError, KeyError):
        date = ''
    #text
    try:
        text1 = soup.find_all("div", class_="opener")
        text1 = text1[0].text
        text2 = soup.find_all("div", class_="bbtext")
        text3 = ''
        for t in text2:
            res = t.find_all("p")
            for r in res:
                text3 = text3 + r.text
        final_text = text1 + text3
        final_text = re.sub("\r\n\s{2,}", "", final_text)
    except IndexError:
        final_text = ''
    #kategorie
    try:
        kategorie = soup.find_all("div", class_="portal-g2a")
        res = kategorie[0].find_all("a")
        kategorie = res[0].text
      #  print(kategorie)
    except IndexError:
        kategorie = ''
    #foto
    try:
        photo_cnt = 0
        div = soup.find_all("div", class_="opener-foto not4bbtext")
        photo = len(div[0].find_all("img"))
        photo_cnt += photo
        div = soup.find_all("div", class_="bbtext")
        photo = len(div[0].find_all("img"))
        photo_cnt += photo
    except IndexError:
        photo_cnt = 0
    #koment
    try:
        div = soup.find_all("ul", class_="art-community")
        koment = div[0].find_all("span")[0].text
        koment = int(re.split(" ", koment[1:-1])[0])
    except IndexError:
        koment = 0
    except ValueError:
        koment = 0
    dic = {}
    dic["Title"] = h1
    dic["Content"] = final_text
    dic["Category"] = kategorie
    dic["Photos_cnt"] = photo_cnt
    dic["Date"] = date
    dic["Comments"] = koment
    return dic

def scrap_web(url):
    """
    Get data from articles
    :param url: url
    :return: data
    """
    links = get_all_links(url, 200)
    db = []
    for link in links:
        db.append(parse_page(link))
    return db

def save_json():
    """
    Save data to json file
    """
    start_time = time.time()
    URL = "https://www.idnes.cz/zpravy/archiv/1?datum=&idostrova=idnes"
    aDict = scrap_web(URL)
    jsonString = json.dumps(aDict, ensure_ascii=False)
    with open("data.json", "w", encoding='utf-8') as jsonFile:
        jsonFile.write(jsonString)
    print("--- %s seconds ---" % round((time.time() - start_time), 2))

if __name__ == '__main__':
    save_json()
