import scrapy
from scrapy.selector import Selector

from scrapy.crawler import CrawlerProcess

import datetime as dt
import os


try:
    os.remove("MongoDbFilm/MongoDbFilm/spiders/csv/top_250_serie.csv")
    print("top_250_serie.csv supprimé.")
except:
    print('Pas de fichier film csv.')




class Top250SpiderSerie(scrapy.Spider):
    name = "top_250"
    
    allowed_domains = ['imdb.com']
    base_url = "https://www.imdb.com/"
    start_urls = ['https://www.imdb.com/chart/toptv/?ref_=nv_tvv_250'] 
    
    max_scraping = 250
    

    def parse(self, response):
        for result in response.css('tr > td.titleColumn')[:self.max_scraping]:
            yield scrapy.Request(url= self.base_url + result.css('a::attr(href)').get(), callback=self.parse_detail)


    def parse_detail(self, response):

        self.url = response
        self.url = str(self.url)
        self.url = self.url.split(' ')
        self.url = self.url[1]
        self.url = self.url.split('/')
        self.url = self.url[0] + "//" + self.url[2] + "/" + self.url[4] + "/" + self.url[5] + "/"
        self.url = self.url + "episodes/?ref_=tt_ov_epl"

        selector = Selector(text=response.text)

        title = response.css('h1::text').extract()

        original_title = str(response.css('div.sc-dae4a1bc-0.gwBsXc::text').get())
        if original_title == "None":
            original_title = title
        else:
            original_title = original_title[16:]


        score = response.css('span.sc-7ab21ed2-1::text').get()
        score = score + '/10'

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

        genre = response.css('a.sc-16ede01-3.bYNgQ.ipc-chip.ipc-chip--on-baseAlt>span::text').get()

        time = response.css('div.sc-80d4314-2>ul>li::text').extract()

        pays = response.css('div.sc-f65f65be-0.ktSkVi>ul.ipc-metadata-list.ipc-metadata-list--dividers-all.ipc-metadata-list--base>li.ipc-metadata-list__item>div.ipc-metadata-list-item__content-container>ul.ipc-inline-list.ipc-inline-list--show-dividers.ipc-inline-list--inline.ipc-metadata-list-item__list-content.base>li.ipc-inline-list__item>a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link::text')[1].extract()

        langue = response.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section/div[2]/ul/li[4]/div/ul/li[1]/a/text()').extract()

        description = response.css('span.sc-16ede01-0.fMPjMP::text').extract()


        yield scrapy.Request(url= self.url,
        callback = self.parse_saison,
        meta = {
            'title':title,
            'original_title':original_title,
            'score':score,
            'annee':annee,
            'public':public,
            'genre':genre,
            'time':time,
            'pays':pays,
            'langue':langue,
            'description':description,
        })


    def parse_saison (self, response):
        title = response.meta['title']
        original_title = response.meta['original_title']
        score = response.meta['score']
        annee = response.meta['annee']
        public = response.meta['public']
        genre = response.meta['genre']
        # episode = response.meta['episode']
        time = response.meta['time']
        pays = response.meta['pays']
        langue = response.meta['langue']
        description = response.meta['description']

        saison = response.css('#episode_top::text').get()
        saison = saison[-2:]

        scrap_type = "serie"

        date = dt.datetime.now()

        yield {
            'date':date,
            'scrap_type':scrap_type,
            'title':title,
            'original_title':original_title,
            'score':score,
            'annee':annee,
            'public':public,
            'genre':genre,
            'saison':saison,
            # 'episode':episode,
            'time':time,
            'pays':pays,
            'langue':langue,
            'description':description,
        }


process_serie = CrawlerProcess(
    settings = {
        'FEEDS':{
            'MongoDbFilm/MongoDbFilm/spiders/csv/top_250_serie.csv':{
                'format':'csv'
            },
        },
        'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/53.0'
    }
)


process_serie.crawl(Top250SpiderSerie)
process_serie.start()
process_serie.stop()