import subprocess
from concurrent.futures import ThreadPoolExecutor


spiders = ['autotrader', 'autotrader2']


def run_spider(spider_file):
    subprocess.run(['scrapy', 'crawl', spider_file])


with ThreadPoolExecutor() as executor:
    executor.map(run_spider, spiders)
