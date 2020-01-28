import requests
from bs4 import BeautifulSoup
import datetime, time
import psycopg2


class ParserYkt:
    # конструктор класса
    def __init__(self):
        self.baseUrl = 'https://forum.ykt.ru/'

    # Подключение к БД PostgerSQL
    def connectToDBParser(self):
        try:
            self.conn = psycopg2.connect(
                    dbname='forum',
                    user='postgres',
                    host='127.0.0.1')
            return 'Database opened successfully'
        except:
            return 'DataBase is not connect'

    # Отключение от БД PostgerSQL
    def closeConnectToDB(self):
        self.conn.close()
        print('Close DB Forum')

    # Создает массив с ссылками
    def parseTheDataAndAdd(self):
        dateTest = datetime.datetime.now()
        global startUrl, nickname, date, title, content
        cursor = self.conn.cursor()
        url = requests.get(self.baseUrl)
        soup = BeautifulSoup(url.content, 'html.parser')
        divs = soup('div', class_='f-main-top_item f-main-top_item--live')
        for a in divs:
            href = a.find('a', class_='emojify')['href']
            startUrl = self.baseUrl + str(href)
            cursor.execute("SELECT link FROM parser")
            cursorQL = cursor.fetchall()
            if (startUrl,) not in cursorQL:
                print(f'Start parsing link {startUrl}')
                getUrlPage = requests.get(startUrl)
                soup = BeautifulSoup(getUrlPage.content, 'html.parser')
                divs = soup.find_all('div', attrs={'class': 'f-view'})
                for div in divs:
                    notAuthorizedUser = div.find('span', class_='topic-view__author anonym')
                    authorizedUser = div.find(class_='f-user_name')
                    title = div.find('div', class_='f-view_title emojify')
                    content = div.find('div', class_='f-view_topic-text emojify')
                    date = datetime.datetime.now().strftime("%Y/%m/%d")
                    # time = div.find('time', class_='f-view_createdate')['datetime']
                    # print(time)
                    if not notAuthorizedUser:
                        pass
                    else:
                        nickname = notAuthorizedUser.text.strip()
                    if not authorizedUser:
                        pass
                    else:
                        nickname = authorizedUser.text.strip()
                try:
                    cursor.execute(
                        'INSERT INTO parser(link, name, date, title, content) VALUES(%s, %s, %s, %s, %s);',
                        (str(startUrl), str(nickname), date, str(title.text.strip()), str(content.text.strip())))
                    print(f"Base add: {startUrl}")
                    self.conn.commit()
                except:
                    print(f'This is link {startUrl} in base!!!')
            else:
                print(f'In base {startUrl}')
        cursor.close()
        print('Close cursor')
        print(datetime.datetime.now() - dateTest)


if __name__ == '__main__':
    g = ParserYkt()
    try:
        g.connectToDBParser()
        while True:
            print('Start Parsing!!!')
            print(g.parseTheDataAndAdd())
            print(f'Time out 25 seconds\n')
            time.sleep(25)
    except(KeyboardInterrupt):
        g.closeConnectToDB()