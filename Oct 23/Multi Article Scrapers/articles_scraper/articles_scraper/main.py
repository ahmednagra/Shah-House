import csv
import glob

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spiders.chien import ChienSpider
from spiders.cleantuesdayparis import CleantuesdayparisSpider
from spiders.enerzine import EnerzineSpider
from spiders.sneakernews import SneakerSpider
from spiders.techno import TechnoSpider
from spiders.tekpolis import TekpolisSpider
from spiders.the_blog import The_blogSpider

from spiders.base import BaseSpider


def start_sequentially(process: CrawlerProcess, crawlers: list):
    print('start crawler {}'.format(crawlers[0].__name__))
    deferred = process.crawl(crawlers[0])

    if len(crawlers) > 1:
        deferred.addCallback(lambda _: start_sequentially(process, crawlers[1:]))


def read_output_files():
    output_files = glob.glob('output/*.csv')
    rows = []

    for output_file in output_files:
        with open(output_file, mode='r', encoding='utf8') as input_file:
            csv_reader = csv.DictReader(input_file)
            for row in csv_reader:
                rows.append(row)

    return rows


if __name__ == '__main__':
    spider_crawlers = [ChienSpider, CleantuesdayparisSpider, EnerzineSpider, SneakerSpider, TechnoSpider,
                       TekpolisSpider, The_blogSpider]

    crawler_process = CrawlerProcess(get_project_settings())
    start_sequentially(crawler_process, spider_crawlers)
    crawler_process.start()
    #
    output_files_read = read_output_files()

    base_spider = BaseSpider()
    base_spider.update_google_sheet(data=output_files_read)
