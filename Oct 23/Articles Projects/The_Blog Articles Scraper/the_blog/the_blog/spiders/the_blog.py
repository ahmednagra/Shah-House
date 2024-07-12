import csv
import os

from scrapy import Request, Spider
from collections import OrderedDict

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gsheets


class The_blogSpider(Spider):
    name = 'the_blog'
    start_urls = ['https://the-blog.fr/']

    headers = {
        'authority': 'the-blog.fr',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                      "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

        self.creds = ServiceAccountCredentials.from_json_keyfile_name('input/google_credentials.json', self.scope)

        self.gsheet_config = self.get_key_values_from_file('input/googlesheet_keys.txt')

        google_sheet_data = self.get_googlesheet_data()
        self.previous_gsheet_scraped_items_urls = [item['URL'] for item in google_sheet_data]
        self.current_scraped_items = []

    def parse(self, response, **kwargs):
        atricles_url = response.css('h2.title a::attr(href), h4.title a::attr(href)').getall()
        for article_url in atricles_url:

            if article_url in self.previous_gsheet_scraped_items_urls:
                continue

            yield Request(url=article_url, callback=self.parse_article, headers=self.headers)

        next_page = response.css('.next.page-numbers::attr(href)').get('')
        if next_page:
            yield Request(url=next_page, callback=self.parse, headers=self.headers)

    def parse_article(self, response):
        item = OrderedDict()

        item['Title'] = response.css('.title .title-span::text').get('').strip()
        item['Intro'] = response.css('.subtitle ::text').get('').strip()
        item['Image'] = response.css('[property="og:image"]::attr(content)').get('')
        item['Content'] = '\n\n\n'.join(response.css(
            'article :not(.code-block):not(.google-auto-placed):not(.su-box):not(.su-box-title):not(.su-box-content):not(script):not(.adsbygoogle)').getall())[:30000]
        item['Category'] = response.css('.category a::text').get('')
        item['Tags'] = ', '.join(response.css('[rel="tag"] ::text').getall())
        item['URL'] = response.url

        self.current_scraped_items.append(item)

    def close(spider, reason):
        spider.update_google_sheet()

    def get_key_values_from_file(self, file_path):
        """
        Get the Google sheet keys and Search URLs keys  from input text file
        """

        with open(file_path, mode='r', encoding='utf-8') as input_file:
            data = {}

            for row in input_file.readlines():
                if not row.strip():
                    continue

                try:
                    key, value = row.strip().split('==')
                    data.setdefault(key.strip(), value.strip())
                except ValueError:
                    pass

            return data

    def get_googlesheet_data(self):
        filename = 'G_Sheet Data/previous_gsheet.csv'

        self.download_google_sheet_data_as_csv(filename)

        with open(filename, mode='r', encoding='utf-8') as data_file:
            data = csv.DictReader(data_file)
            columns = data.fieldnames
            return [row for row in data]

    def download_google_sheet_data_as_csv(self, filename):
        spreadsheet_id = self.gsheet_config.get('googlesheet_id')

        gsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

        sheets = gsheets.Sheets(self.creds)
        sheet = sheets.get(gsheet_url)
        sheet[0].to_csv(filename)

    def update_google_sheet(self):
        if not self.current_scraped_items:
            self.logger.debug('\n\nThere is no new article found...!!!\n\n')
            return

        columns = [[col for col in row.keys()] for row in self.current_scraped_items][:1]
        rows_values = [[value for value in row.values()] for row in self.current_scraped_items]

        spreadsheet_id = self.gsheet_config.get('googlesheet_id')
        tab_sheet_name = self.gsheet_config.get(f'tab_name')

        service = build('sheets', 'v4', credentials=self.creds)
        sheet_range = tab_sheet_name

        gsheet_headers = self.gsheet_fileheaders()

        if not gsheet_headers:
            gsheet_headers = columns[0]

            # Append headers
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                body={
                    "majorDimension": "ROWS",
                    "values": [gsheet_headers]  # Add headers as a single row
                },
                valueInputOption="USER_ENTERED"
            ).execute()

        # append data
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=sheet_range,
            body={
                "majorDimension": "ROWS",
                "values": rows_values  # representing each row values as list. So it contains as list of lists
            },
            valueInputOption="USER_ENTERED"
        ).execute()

        self.logger.debug(f'\n\nNew Articles Found: "{len(self.current_scraped_items)}"')
        self.logger.debug(f'Google Sheet "{tab_sheet_name}" has been updated\n\n')

    def gsheet_fileheaders(self):
        try:
            csv_file_path = 'G_Sheet Data/previous_gsheet.csv'
        except:
            csv_file_path = ''

        existing_headers = []
        if os.path.exists(csv_file_path):
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                existing_headers = next(reader, [])  # Read the first row as headers

        return existing_headers
