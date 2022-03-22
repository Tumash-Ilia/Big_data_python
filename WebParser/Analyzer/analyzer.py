import re
from pymongo import MongoClient
from datetime import datetime

client = MongoClient('localhost', 27017)
db = client.dpbcviceni
collection = db.articles


def docs_count():
    """
    Pocet clanku
    """
    print("Pocet clanku:", collection.estimated_document_count())


def docs_duplicate():
    """
    Duplicitni clanky
    """
    res = collection.find({}, {'Title': 1, "_id": 0})
    d = {}
    cnt = 0
    for r in res:
        title = r['Title']
        if title not in d:
            d.setdefault(title, 1)
        else:
            cnt += 1

    print("Pocet duplicitnich clanku:", cnt)


def most_comm():
    """
    Nejvic komentari
    """
    res = collection.find({}, {'Title': 1, 'Comments': 1, "_id": 0})
    maximum = 0
    title = ''
    for r in res:
        coment = int(r['Comments'])
        if coment > maximum:
            title = r['Title']
            maximum = coment
    print('Jmeno clanku s nejvice komentari:', title, "\nPocet:", maximum)


def most_foto():
    """
    Nejvic foto
    """
    res = collection.find({}, {'Photos_cnt': 1, "_id": 0})
    maximum = 0
    for r in res:
        photo_cnt = int(r['Photos_cnt'])
        if photo_cnt > maximum:
            maximum = photo_cnt
    print('Nejvyssi pocet foto:', maximum)


def year_cnt():
    """
    Pocet clanku podle roku publikace
    """
    res = collection.find({}, {'Date': 1, "_id": 0})
    d = {}
    for r in res:
        date = r['Date']
        if date != '':
            date = re.match('\d{4}', date).group()
        if date not in d:
            d.setdefault(date, 1)
        else:
            d[date] += 1
    for key, value in d.items():
        print("Rok publikace:", key if key!='' else 'Unknown', "Pocet clanku:", value)


def cat_cnt():
    """
    Pocet kategorie
    """
    res = collection.find({}, {'Category': 1, "_id": 0})
    cat = {}
    for r in res:
        category = r['Category']
        if category not in cat:
            cat.setdefault(category, 1)
        else:
            cat[category] += 1
    print("Pocet kategorii:", len(cat))
    for key, value in cat.items():
        print("Kategorie:", key if key != '' else 'Unknown', "\tPocet clanku:", value)


def mcw_title():
    """
    5 nejcastejsich slov v nazvu 21
    """
    res = collection.find({}, {'Title': 1, 'Date': 1, "_id": 0})
    words_arr = {}
    for r in res:
        title = r['Title']
        date = r['Date']
        if date != '':
            date = re.match('\d{4}', date).group()
        if date == '2021':
            words = title.lower().split()
            for w in words:
                if w not in words_arr and len(w) > 4:
                    words_arr.setdefault(w, 1)
                elif len(w) > 4:
                    words_arr[w] += 1

    sorted_dict = dict(sorted(words_arr.items(), key=lambda item: item[1]))
    for x in list(reversed(list(sorted_dict)))[0:5]:
        print("Slovo:", x, "\tPocet vyskitu:", sorted_dict[x])




def mcw_text():
    """
    8 Nejcastejsich slov v clancich >4
    """
    res = collection.find({}, {'Content': 1, "_id": 0})
    words_arr = {}
    for r in res:
        content = r['Content']

        words = content.lower().split()
        for w in words:
            if w not in words_arr and len(w) > 4:
                words_arr.setdefault(w, 1)
            elif len(w) > 4:
                words_arr[w] += 1

    sorted_dict = dict(sorted(words_arr.items(), key=lambda item: item[1]))
    for x in list(reversed(list(sorted_dict)))[0:8]:
        print("Slovo:", x, "\tPocet vyskitu:", sorted_dict[x])


def oldest_art():
    """
    Datum nejstarseho clanku
    """
    res = collection.find({}, {'Date': 1, "_id": 0})
    dates = []
    for r in res:
        date = r['Date']
        if date != '':
            dates.append(datetime.strptime(date, "%Y-%m-%d").date())
    print("Datum nejstarseho clanku:", min(dates))



def total_com():
    """
    Celkovy pocet komentaru
    """
    res = collection.find({}, {'Comments': 1, "_id": 0})
    cnt = 0
    for r in res:
        coment = int(r['Comments'])
        cnt += coment
    print('Celkovy pocet komentaru:', cnt)


def total_words():
    """
    Celkovy pocet slov v clancich
    """
    res = collection.find({}, {'Content': 1, "_id": 0})
    words_arr = {}
    cnt = 0
    for r in res:
        content = r['Content']
        words = content.lower().split()
        cnt += len(words)
        for w in words:
            if w not in words_arr and len(w) > 4:
                words_arr.setdefault(w, 1)
            elif len(w) > 4:
                words_arr[w] += 1
    print("Celkovy pocet unikatnich slov:",  '{0:,}'.format(len(words_arr)))
    print("Celkovy pocet vsech slov:", '{0:,}'.format(cnt))

def bonus():
    res = collection.find({}, {'Title': 1,'Content': 1, "_id": 0})
    cov = {}
    for r in res:
        content = r['Content'].lower()
        title = r['Title']
        kov_cnt = len(re.findall(r'covid\S*', content))
        if kov_cnt != 0:
            cov.setdefault(title, kov_cnt)

    sorted_dict = dict(sorted(cov.items(), key=lambda item: item[1]))
    for x in list(reversed(list(sorted_dict)))[0:3]:
        print("Title:", x, "\tPocet vyskitu covid-19:", sorted_dict[x])


def b_print(num):
    print("-------- {} --------".format(num))

if __name__ == '__main__':
    # Pocet clanku
    b_print(1)
    docs_count()

    # Pocet duplicitnich clanku
    b_print(2)
    docs_duplicate()

    # Jmeno clanku s nejvice koment
    b_print(3)
    most_comm()

    # Nejvyssi pocet foto
    b_print(4)
    most_foto()

    # Pocet clanku podle roku publikace
    b_print(5)
    year_cnt()

    # Pocet unikatnich kategorie a pocet clanku v kategorii
    b_print(6)
    cat_cnt()

    # 5 nejcastejsich slov v nazvu clanku z roku 21
    b_print(7)
    mcw_title()

    # 8 Nejcastejsich slov v clancich
    b_print(8)
    mcw_text()

    # Datum nejstarseho clanku
    b_print(9)
    oldest_art()

    # Celkovy pocet komentaru
    b_print(10)
    total_com()

    # Celkovy pocet slov v clancich
    b_print(11)
    total_words()
    b_print('Bx onus')
    # Bonus
    bonus()
    pass