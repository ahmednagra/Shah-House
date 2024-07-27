import re
import json
from collections import OrderedDict
from scrapy import Request, Spider, Selector


class MembersSpiderSpider(Spider):
    name = "members_spider"
    allowed_domains = ["www.acq.org"]
    # start_urls = ["https://www.acq.org/repertoire-des-membres/"]
    start_urls = ["https://www.acq.org/wp-admin/admin-ajax.php"]

    cookies = {
        'REGIONAL_ACQ': '0',
        'cmplz_consented_services': '',
        'cmplz_policy_id': '34',
        'cmplz_marketing': 'allow',
        'cmplz_statistics': 'allow',
        'cmplz_preferences': 'allow',
        'cmplz_functional': 'allow',
        'cmplz_banner-status': 'dismissed',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'REGIONAL_ACQ=0; cmplz_consented_services=; cmplz_policy_id=34; cmplz_marketing=allow; cmplz_statistics=allow; cmplz_preferences=allow; cmplz_functional=allow; cmplz_banner-status=dismissed',
        'Origin': 'https://www.acq.org',
        'Pragma': 'no-cache',
        'Referer': 'https://www.acq.org/repertoire-des-membres/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    def start_requests(self):
        yield Request(url=self.start_urls[0], headers=self.headers, cookies=self.cookies,
                      body=json.dumps(self.get_formdata()), method='POST')

    def parse(self, response):
        pass

    def get_formdata(self):
        data = {
            'action': 'get_member_directory_members_ajax',
            'filters[keywords]': '',
            'filters[region]': '',
            'filters[city]': '',
        }

        return data
