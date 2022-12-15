import scrapy
from scrapy.crawler import CrawlerProcess

import datetime as dt
import os


try:
    os.remove("MongoDbFilm/MongoDbFilm/spiders/csv/top_250_film.csv")
    print("top_250_film.csv supprimé.")
except:
    print('Pas de fichier film csv.')




class Top250Spider(scrapy.Spider):
    name = "Top250Spider"
    
    allowed_domains = ['imdb.com']
    base_url = "https://www.imdb.com/"
    start_urls = ['https://www.imdb.com/chart/top/?ref_=nv_mv_250']


    scraping = 250
    max_acteur = 80

    
    def parse(self, response):
        for result in response.css('tr > td.titleColumn')[:self.scraping]:
            yield scrapy.Request(url= self.base_url + result.css('a::attr(href)').get(), callback=self.parse_detail)


    def parse_detail(self, response):
        title = response.css('h1::text').extract()

        original_title = str(response.css('div.sc-dae4a1bc-0.gwBsXc::text').get())
        if original_title == "None":
            original_title = title
        else:
            original_title = original_title[16:]

        score = response.css('span.sc-7ab21ed2-1::text').get()

        annee = response.css('div.sc-80d4314-2>ul>li>span::text').extract()[0]
        public = response.css('div.sc-80d4314-2>ul>li>span::text').extract()[1]

        if public == 'G' or public == 'PG' or public == 'TV-PG' or public == 'TV-G' or public == 'PG-13' or public == 'Not Rated' or public == 'Passed' or public == 'Approved':
            public = 'Tous publics'
        elif public == 'R':
            public = 'Tous publics avec avertissement'
        elif public == 'TV-Y':
            public = '2+'
        elif public == 'TV-Y7-FV' or public == 'TV-Y7':
            public = '7+'
        elif public == 'GP':
            public = '12+'
        elif public == 'TV-14':
            public = '14+'
        elif public == 'TV-MA' or public == 'Unrated':
            public = '16+'
        elif public == 'X':
            public = '18+'
            
        time = response.css('div.sc-80d4314-2>ul>li::text').extract()

        description = response.css('span.sc-16ede01-0.fMPjMP::text').extract()

        genre = response.css('a.sc-16ede01-3.bYNgQ.ipc-chip.ipc-chip--on-baseAlt>span::text').get()

        pays = response.css('div.sc-f65f65be-0.ktSkVi>ul.ipc-metadata-list.ipc-metadata-list--dividers-all.ipc-metadata-list--base>li.ipc-metadata-list__item>div.ipc-metadata-list-item__content-container>ul.ipc-inline-list.ipc-inline-list--show-dividers.ipc-inline-list--inline.ipc-metadata-list-item__list-content.base>li.ipc-inline-list__item>a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link::text')[1].extract()

        langue = response.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section/div[2]/ul/li[4]/div/ul/li/a/text()').extract()

        
        yield scrapy.Request(url= self.base_url + response.xpath('//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[1]/div[3]/ul/li[3]/a[1]/@href').get(), callback=self.parse_acteur, meta={
            'title':title,
            'original_title':original_title,
            'score':score,
            'annee':annee,
            'public':public,
            'time':time,
            'description':description,
            'genre':genre,
            'pays':pays,
            'langue':langue,
        })


    def parse_acteur (self, response):
        title = response.meta['title']
        original_title = response.meta['original_title']
        score = response.meta['score']
        annee = response.meta['annee']
        public = response.meta['public']
        time = response.meta['time']
        description = response.meta['description']
        genre = response.meta['genre']
        pays = response.meta['pays']
        langue = response.meta['langue']


        list_acteur = response.css('.primary_photo+ td a::text').extract()
        la_liste_acteur = []
        if self.max_acteur > len(list_acteur):
            self.max_acteur = len(list_acteur)
        for acteur in list_acteur[:self.max_acteur]:
            la_liste_acteur.append(acteur.replace("\n", ""))

        scrap_type = "film"

        date = dt.datetime.now()

        yield {
            'date':date,
            'scrap_type':scrap_type,
            'title':title,
            'original_title':original_title,
            'genre':genre,
            'score':score,
            'annee':annee,
            'public':public,
            'time':time,
            'langue':langue,
            'pays':pays,
            'description':description,
            'acteur':la_liste_acteur,
        }


process_base = CrawlerProcess(
    settings = {
        'FEEDS':{
            'MongoDbFilm/MongoDbFilm/spiders/csv/top_250_film.csv':{
                'format':'csv'
            }
        },
        'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
    }
)



process_base.crawl(Top250Spider)
process_base.start()
process_base.stop()