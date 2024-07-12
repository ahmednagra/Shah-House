import csv
import glob

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from twisted.internet import defer
from twisted.internet import reactor

from spiders.aktialkv import AktialkvSpider
from spiders.blok import BlokSpider
from spiders.bo import BospiderSpider
from spiders.centrallkv import centrallkvSpider
from spiders.huoneistokeskus import HuoneistokeskusSpider
from spiders.kiinteistomaailma import KiinteistomaailmaSpider
from spiders.kotimeklarit import KotimeklaritSpider
from spiders.neliotliikkuu import neliotliikkuuSpider
from spiders.qvadrat import QvadratSpider
from spiders.remax import RemaxSpider
from spiders.rivehomes import RivehomesSpider
from spiders.roof import RoofSpider
from spiders.solidhouse import SolidhouseSpider
from spiders.sothebysrealty import SothebysrealtySpider
from spiders.westhouse import WesthouseSpider

from spiders.methods import comparison_excel


def start_sequentially(process: CrawlerProcess, crawlers: list):
    deferreds = []
    for crawler in crawlers:
        print('start crawler {}'.format(crawler.__name__))
        deferred = process.crawl(crawler)
        deferreds.append(deferred)

    return defer.DeferredList(deferreds)


def read_input_file():
    input_file = 'input/scraper_status.csv'

    with open(input_file, mode='r', encoding='utf8') as input_file:
        return list(csv.DictReader(input_file))


def read_output_files():
    output_files = glob.glob('output/properties/*.csv')
    rows = []

    for output_file in output_files:
        with open(output_file, mode='r', encoding='utf8') as input_file:
            csv_reader = csv.DictReader(input_file)
            for row in csv_reader:
                rows.append(row)

    return rows


if __name__ == '__main__':
    csv_rows = read_input_file()

    # Dont run and process those spiders which are not set to True in the csv file
    dont_run_crawlers = [row['SpiderName'].strip() for row in csv_rows if 'true' not in row.get('Process', '').lower()]

    spider_crawlers = [AktialkvSpider, BlokSpider, BospiderSpider,
                       centrallkvSpider,
                       HuoneistokeskusSpider, KiinteistomaailmaSpider,
                       KotimeklaritSpider, neliotliikkuuSpider, QvadratSpider,
                       RemaxSpider, RivehomesSpider, RoofSpider,
                       SolidhouseSpider, SothebysrealtySpider, WesthouseSpider
                       ]

    crawlers = [crawler for crawler in spider_crawlers if crawler.name not in dont_run_crawlers]

    crawler_process = CrawlerProcess(get_project_settings())
    d = start_sequentially(crawler_process, crawlers)

    d.addBoth(lambda _: reactor.stop())
    reactor.run()

    output_files_read = read_output_files()
    file = comparison_excel(data=output_files_read)
