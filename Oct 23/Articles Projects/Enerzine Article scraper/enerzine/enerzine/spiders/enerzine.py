import csv
import os

from scrapy import Request, Spider
from collections import OrderedDict

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gsheets


class EnerzineSpider(Spider):
    name = 'enerzine'
    start_urls = ['https://www.enerzine.com/']

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
        article_urls = response.css('.wp-block-post-title a::attr(href), .featured_title_over > a::attr(href), .featured_title_over h2 a::attr(href), h2.entry-title > a::attr(href)').getall() or []
        for article in article_urls:

            if article in self.previous_gsheet_scraped_items_urls:
                continue

            yield Request(url=article, callback=self.parse_article)

    def parse_article(self, response):

        item = OrderedDict()

        item['Category'] = ', '.join(response.css('.rank-math-breadcrumb a ::text').getall()[1:])
        item['Title'] = response.css('h1.entry-title::text').get('').strip()
        item['Intro'] = ' '.join(response.css('.entry-content > p strong::text').getall())
        item['Image'] = response.css('[property="og:image"]::attr(content)').get('')
        item['Credits (sources)'] = ' '.join(response.css('div.entry-content p:contains("Source :") ::text').getall())
        item['Tags'] = ', '.join(response.css('.meta-tags a::text').getall())
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
