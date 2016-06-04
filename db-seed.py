import requests
from bs4 import BeautifulSoup
import json
import time
import random
import unicodedata
import os
import psycopg2
import urlparse


class Player:
    
    def __init__(self, last_name, full_name, url, team):
        self.last_name = last_name
        self.full_name = full_name
        self.url = url
        self.team = team
        self.alt_last = ''
        self.alt_full = ''
        self.scraped = False
    
    def addStats(self, player_attr):
        self.player_attr = player_attr
        self.scraped = True
    
    def addAltName(self, alt_last, alt_full):
        self.alt_last = alt_last
        self.alt_full = alt_full


class dbWrapper:
    def __init__(self):
        urlparse.uses_netloc.append("postgres")
        url = urlparse.urlparse(os.environ["DATABASE_URL"])

        self.conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    
    def addPlayersTable(self, attr_list):
        cur = self.conn.cursor()
        comm = "CREATE TABLE Players (Last_Name varchar(50), Full_Name varchar(50), URL varchar(50), Team varchar(50), Alt_Last varchar(50), Alt_Full varchar(50)"
        for x in range(len(attr_list)):
            for y in range(len(attr_list[x])):
                comm += ", {} int".format(attr_list[x][y].replace(' ', '_'))
        comm += ");"
        cur.execute(comm)    
        self.conn.commit()
        
    def getURLs(self):
        URLs = []
        cur = self.conn.cursor()
        comm = 'SELECT URL FROM Players'
        cur.execute(comm)
        for row in cur:
            URLs.append(row)
        return URLs
    
    def addPlayers(self, player_list, attr_list):
        cur = self.conn.cursor()
        comm = 'INSERT INTO Players (Last_Name, Full_Name, URL, Team, Alt_Last, Alt_Full'
        for x in range(len(attr_list)):
            for y in range(len(attr_list[x])):
                comm += ", {} int".format(attr_list[x][y].replace(' ', '_'))
        comm += ')\nVALUES '
        for player in player_list:
            if player.scraped:
                comm += '('
                for x in range(len(player_list)):
                    for y in range(len(player_list[x])):
                        comm += str(player_list[x][y])
                        comm +=  ','
                comm.replace(',', "", (string.count(',')-1))
                comm += '),'
                
        comm.replace(',', "", (string.count(',')-1))
        cur.execute(comm)
        self.conn.commit()
    def closeDB(self):
        self.conn.close()
"""
Scrapes list of active players for their full name, last name, team, and url suffix for stat page. 
Creates a player object and adds them to player list.
"""

r = requests.get('http://www.premierleague.com/content/premierleague/en-gb/players/index.html')

player_list = []
for index in range(1, 9):
    url = ('http://www.premierleague.com/ajax/player/index/BY_CLUB/null/null/null/null/null/ALL/2015-2016/null/null/100/4/2/2/{}/null.json').format(index)
    r = requests.get(url)
    if r.status_code != 200:
        break
    c = r.content
    table = json.loads(c)
    players = table["playerIndexSection"]["index"]["resultsList"]

    for item in players:
        try:
            if item['lastSeason']['season'] == '2015-2016':
                last_name = ''.join(item['lastName'])
                full_name = ''.join(item['fullName'])
                url = ''.join(item['cmsAlias'])
                team = ''.join(item['club']['clubFullName'])
                player = Player(last_name, full_name, url, team)
                player_list.append(player)
        except Exception as e:
            print "EXCEPTION"
            pass
        
    n = (random.random() * random.randint(1, 3) ) + 6
    time.sleep(n)
    
    print "On page {}".format(index)

"""
Goes through player list and identifies players with non-ascii characters and converts them to ascii equivalent, stores them in alt fields
Probably a more efficient and/or eloquent way but it works.
"""

for player in player_list:
    try: 
        print "{} {} {} {}".format(player.full_name, player.last_name, player.url, player.team)
    except:
        print player.full_name
        print player.last_name
        alt_last = unicodedata.normalize('NFD', player.last_name).encode('ascii', 'ignore')
        alt_full = unicodedata.normalize('NFD', player.full_name).encode('ascii', 'ignore')
        print alt_last
        print alt_full
        player.addAltName(alt_last, alt_full)
        


"""
Uses player url suffix to scrape each player's stat page
"""
             
section_list = ["clubsTabsAttacking", "clubsTabsDefending", "clubsTabsDisciplinary" ]
section_attr = [["Goals", "Shots", "Pens Scored", "Assists", "Crosses", "Offsides"], ["Saves Made", "Own Goals", "Clean Sheets", "Blocks", "Clearances"], ["Fouls", "Cards"]]

db = dbWrapper()
# db.addPlayersTable(section_attr)


scrape_count = 0
failed_players = []
try:
    for player in player_list:
        url = 'http://www.premierleague.com/en-gb/players/profile.statistics.html/{}'.format(player.url)
        r = requests.get(url)
        if r.status == 500:
            while r.status == 500:
                print "HTTP 500 Code. Attempting url again."    
                r = requests.get(url)
        c = r.content
        soup = BeautifulSoup(c, "html.parser")
        player_attr= [[0,0,0,0,0,0], [0,0,0,0,0], [0,0]]

        for index in range(0, 3):
            div = soup.find(id=section_list[index])
            try:
                save_next_div = False
                for x in div.find_all('li'):
                    for y in x.contents:
                        if y.name == 'div':
                            if ''.join(y['class']) == 'label':
                                if y.get_text() in section_attr[index]:
                                    save_next_div = True
                                    attr_index = section_attr[index].index(y.get_text())
                            elif ''.join(y['class']) == 'data':
                                if save_next_div == True:
                                    player_attr[index][attr_index] = y.get_text()
                                    save_next_div = False


            except Exception as e:
                print e
                print "This player url failed {}".format(player.url)
                failed_players.append(player.url)
                print soup.prettify()
                break
        
        
        print player.full_name
            for x in range(len(player_attr)):
                for y in range(len(player_attr[x])):
                    print "Stat Name: {}, Stat Data: {}".format(section_attr[x][y], player_attr[x][y])        
        
        scrape_count += 1
        print "Num players scraped {}".format(scrape_count)
        print "Num failed players {}".format(len(failed_players))
        print "Num players to be scraped {}".format(len(player_list))
        n = (random.random() * random.randint(1, 3) ) + 6
        time.sleep(n)
        
except Exception as e:
    print item
    print e
    print r.status
    print soup.prettify()
        
finally:
    db.addPlayers(player_list, section_attr)
    db.close
    
    
       
      
