# picking marks from fantlab
def logat(ltime, lurl, lstatus):
    with open('d:\\fantlab\\load2.log', 'at') as log:
        log.write(str(ltime)+'\t'+str(lurl)+'\t'+str(lstatus)+'\n')
conn = psycopg2.connect("dbname=x user=x password=x")
cur = conn.cursor()
cur.execute("select coalesce(max(user_id),0)+1 from fantlab.user_ratings;")
user_id = int(cur.fetchone()[0])
page = 1

base_url = 'https://fantlab.ru'

headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
      }

for uid in range(user_id,user_id + 50000):
        page = 1
        url = base_url +'/user%d/markspage%d' % (uid, page)
        r = requests.get(url, headers = headers)
        time.sleep(random.randint(1, 2)/2)
        logat(time.ctime(),url, r.status_code)
        while r.status_code == 200:
            text = r.text
            soup = BeautifulSoup(text,"lxml")
            marks_list = soup.find('table', {'class': 'v9b'})
            items = marks_list.find_all('tr',)
            for item in items:
                try:
                    book_auth = item.find('a').previousSibling
                    book_auth = book_auth.replace(re.findall('\d+\.',book_auth)[0],'').strip()
                    book_link = base_url + item.find('td').find('a').get('href')
                    book_name = item.find('td').find('a').text
                    book_id = re.findall('\d+', book_link)[0]
                    user_rating = item.find('td', {'align': 'center'}).text            
                    #print((uid,book_auth,book_link, book_name,book_id,user_rating))
                    cur.execute("INSERT INTO fantlab.user_ratings(user_id, book_auth, book_link, book_name, book_id, user_rating)VALUES(%s, %s, %s, %s, %s, %s)",(uid,book_auth,book_link, book_name,book_id,user_rating))
        
                except:
                    pass

            page += 1
            url = base_url +'/user%d/markspage%d' % (uid, page)
            r = requests.get(url, headers = headers) 
            time.sleep(random.randint(1, 2)/2)
            logat(time.ctime(),url, r.status_code)
            conn.commit()
                        
conn.commit()
cur.close()
conn.close()         