import os
import re
from collections import OrderedDict

from scrapy import Spider, Request


class BaseSpider(Spider):
    name = 'base'
    start_urls = ['']

    data = []

    def parse(self, response, **kwargs):
        pass

    def get_item(self, response_sel):
        item = OrderedDict()
        item['Address'] = self.get_address(response_sel)
        item['street number'] = self.get_street_number(response_sel)
        item['Type'] = self.get_price(response_sel)  # price
        item['Rooms'] = self.get_rooms(response_sel)
        item['Other'] = ''
        item['Size (m2)'] = self.get_size(response_sel)
        item['Agency url'] = self.get_agency_url(response_sel)
        item['Agency name'] = self.get_static(response_sel)
        self.data.append(item)

        return item

    def get_address(self, response):
        pass

    def get_street_number(self, response):
        pass

    def get_price(self, response):
        pass

    def get_rooms(self, response):
        pass

    def get_size(self, response):
        pass

    def get_agency_url(self, response):
        pass

    def get_static(self, response):
        pass

    # def closed(self, reason):
    #     sheet_name = 'SCRAPEDATA'
    #     file_name = self.write_to_excel(self.data, sheet_name) # pass here append mood
