import re
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import ticker
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.dpbcviceni
collection = db.articles

def art_in_time():
    """
    Pridavani clanku v case
    """
    res = collection.find({}, {'Date': 1, "_id": 0})

    d = {}
    for r in res:
        date = r['Date']
        if date != '':
            date = datetime.strptime(date, "%Y-%m-%d").date()
            if date not in d:
                d.setdefault(date, 1)
            else:
                d[date] += 1
    x = []
    y = []
    for key in sorted(d):
        x.append(key)
        y.append(d[key])

    x = x[3::10]
    y = y[3::10]
    x = mdates.date2num(x)
    fig, ax = plt.subplots()

    ax.plot(x, y)

    #Axis
    fmt_half_year = mdates.MonthLocator(interval=3)
    ax.xaxis.set_major_locator(fmt_half_year)
    fmt_month = mdates.MonthLocator()
    ax.xaxis.set_minor_locator(fmt_month)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()
    ax.yaxis.set_major_locator(ticker.MultipleLocator(20))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(5))
    #Labels
    ax.set_title('Pridavani clanku v case')
    ax.set_xlabel('Datum')
    ax.set_ylabel('Pocet clanku')
    plt.grid()
    plt.show()

def art_in_years():
    """
    Pocet clanku v jednotlivych rocich
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

    x = []
    y = []
    for key in sorted(d):
        x.append(key)
        y.append(d[key])
        #print(key, d[key])

    fig, ax = plt.subplots()

    ax.bar(x, y)

    ax.set_facecolor('seashell')
    fig.set_facecolor('floralwhite')
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5000))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(500))

    for index, value in enumerate(y):
        plt.text(index-0.25, value+100 ,str(value))

    ax.set_title('Pocet clanku v jednotlivych rocich')
    ax.set_xlabel('Rok')
    ax.set_ylabel('Pocet clanku')
    plt.show()



def art_len_comm():
    """
    Vztah mezi delkou a poctem komentaru
    """
    res = collection.find({}, {'Content': 1, 'Comments': 1, "_id": 0})
    d = []
    marks = '''!()-[]{};?@#$%:'"\,./^&amp;*_'''
    for r in res:
        cont = r['Content']
        comm = r['Comments']
        for x in cont:
            if x in marks:
                cont = cont.replace(x, " ")
        d.append((len(cont.split()), comm))
    x = []
    y = []
    for k in d:
        x.append(k[0])
        y.append(k[1])


    fig, ax = plt.subplots()
    ax.scatter(x, y, c='deeppink')

    ax.set_facecolor('black')

    plt.xticks(fontsize=24)
    plt.yticks(fontsize=24)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1000))
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(100))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(1000))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(100))

    ax.set_title('Vztah mezi delkou clanku a poctem komentaru',fontsize=32)
    ax.set_xlabel('Delka clanku (Slov)',fontsize=32)
    ax.set_ylabel('Pocet clanku',fontsize=32)
    fig.set_figwidth(10)
    fig.set_figheight(10)
    plt.show()


def art_in_cat():
    """
    Podil clanku v kategoriich
    """
    res = collection.find({}, {'Category': 1, "_id": 0})
    cat = {}
    for r in res:
        category = r['Category']
        if category not in cat:
            cat.setdefault(category, 1)
        else:
            cat[category] += 1
    cnt = 0
    for v in cat.values():
        cnt += v
    x = []
    y = []
    for key in cat:
        x.append(key)
        y.append(round(cat[key]/cnt, 3))
    myexplode = [0.1] * len(y)
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    x[10] = "Ostatni"
    # myexplode[y.index(max(y))] = 0.2
    plt.pie(y, labels=x, explode=myexplode, rotatelabels=True, radius=1.1,normalize=False)
    fig.set_figwidth(14)
    fig.set_figheight(14)
    plt.show()

def hist_word_cnt():
    """
    Histogram poctu slov v clancich
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
    # print(len(words_arr)) 1268212
    sorted_dict = dict(sorted(words_arr.items(), key=lambda item: item[1]))
    x = []
    y = []

    for w in list(reversed(list(sorted_dict)))[0:500]:
        x.append(w)
        y.append(sorted_dict[w])
    fig, ax = plt.subplots()

    ax.yaxis.set_major_locator(ticker.MultipleLocator(5000))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(500))
    plt.xticks(rotation=90)
    fig.set_figwidth(80)
    ax.set_title('Histogram poctu slov v clancich', fontsize=12)
    ax.set_xlabel('Slova', fontsize=4)
    ax.set_ylabel('Pocet', fontsize=4)
    ax.bar(x, y, width=0.2)
    plt.show()

def hist_word_cnt2():
    """
    Histogram poctu slov v clancich
    """
    res = collection.find({}, {'Content': 1, "_id": 0})
    words_arr = []
    marks = '''!()-[]{};?@#$%:'"\,./^&amp;*_'''
    for r in res:
        content = r['Content']
        for x in content:
            if x in marks:
                content = content.replace(x, " ")
        words = content.lower().split()
        words_arr.append(len(words))

    fig, ax = plt.subplots()

    ax.set_title('Histogram poctu slov v clancich', fontsize=12)
    ax.set_ylabel('Pocet clanku', fontsize=12)
    ax.set_xlabel('Pocet slov', fontsize=12)
    plt.xlim(-50, 4000)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(500))
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(100))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(500))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(100))
    ax.hist(words_arr,density=False,bins=100)
    plt.show()

def hist_word_len():
    """
    Histogram pro delku slov v clancich
    """
    res = collection.find({}, {'Content': 1, "_id": 0})
    words_arr = {}
    marks = '''!()-[]{};?@#$%:'"\,./^&amp;*_'''
    for i in range(26):
        words_arr.setdefault(i, 0)
    for r in res:
        content = r['Content']
        for x in content:
            if x in marks:
                content = content.replace(x, " ")
        words = content.lower().split()
        for w in words:
            if len(w) < 25:
                words_arr[len(w)] += 1

    # sorted_dict = dict(sorted(words_arr.items(), key=lambda item: item[1]))
    x = []
    y = []

    # for w in list(reversed(list(sorted_dict)))[0:500]:
    #     x.append(w)
    #     y.append(sorted_dict[w])
    for w in words_arr:
        x.append(w)
        y.append(words_arr[w])
    fig, ax = plt.subplots()

    ax.set_title('Histogram pro delku slov v clancich')
    ax.set_xlabel('Delka slov')
    ax.set_ylabel('Pocet(kk)')
    ax.bar(x, y)
    plt.show()

def words_in_title():
    """
    Vyskyt slova koronavirus\vakcina v nazvu
    """
    res = collection.find({}, {'Title': 1, 'Date': 1, "_id": 0})
    cov = {}
    vac = {}
    for r in res:
        title = r['Title'].lower()
        date = r['Date']
        kov_cnt = len(re.findall(r'covid\S*', title))
        vac_cnt = len(re.findall(r'vakcín\S*', title))
        if kov_cnt != 0:
            if date != '':
                date_t = datetime.strptime(date, "%Y-%m-%d").date()
                if date_t not in cov:
                    cov.setdefault(date_t, kov_cnt)
                else:
                    cov[date_t] += kov_cnt
        if vac_cnt != 0:
            if date != '':
                date_t = datetime.strptime(date, "%Y-%m-%d").date()
                if date_t not in vac:
                    vac.setdefault(date_t, vac_cnt)
                else:
                    vac[date_t] += vac_cnt

    fig, ax = plt.subplots()
    x = []
    y = []
    for key in sorted(cov):
        x.append(key)
        y.append(cov[key])
    x = mdates.date2num(x)

    x1 = []
    y1 = []
    for key in sorted(vac):
        x1.append(key)
        y1.append(vac[key])
    x1 = mdates.date2num(x1)


    ax.plot(x, y,  'b')
    ax.plot(x1, y1, 'r')

    # Axis
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))
    # Labels
    ax.set_title('Vyskyt slova koronavirus\\vakcina v nazvu')
    ax.set_xlabel('Datum')
    ax.set_ylabel('Pocet vyskytu')
    plt.grid()
    plt.show()

def bonus():
    res = collection.find({}, {'Date': 1, "_id": 0})

    d = {}
    for r in res:
        date = r['Date']
        if date != '':
            date = datetime.strptime(date, "%Y-%m-%d").date().weekday()
            if date not in d:
                d.setdefault(date, 1)
            else:
                d[date] += 1
    x = []
    y = []
    for key in sorted(d):
        x.append(key)
        y.append(d[key])

    fig, ax = plt.subplots()


    ax.yaxis.set_minor_locator(ticker.MultipleLocator(200))
    ax.set_title('Histogram poctu clanku v dnech tydnu')
    ax.set_xlabel('Den v tydnu')
    ax.set_ylabel('Pocet clanku')
    plt.xticks(range(7), ['po', 'út', 'st', 'čt', 'pá', 'so', 'ne'])
    ax.bar(x, y)
    plt.show()

if __name__ == '__main__':
    #Pridavani clanku v case
    art_in_time()

    #Pocet clanku v jednotlivych rocich
    art_in_years()

    #Vztah mezi delkou clanku a poctem komentaru
    art_len_comm()

    #Podil clanku v kategoriich
    art_in_cat()

    #Histogram poctu slov v clancich
    #hist_word_cnt()
    hist_word_cnt2()

    #Histogram pro delku slov v clancich
    hist_word_len()

    #Vyskyt slova koronavirus\vakcina v nazvu
    words_in_title()

    #Bonus
    bonus()

    pass