import os
import re
import glob
import urllib.parse
from time import sleep, time
from datetime import datetime
from collections import OrderedDict

import requests
from scrapy import Spider, Request, Selector, signals

# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import Select
# from selenium.webdriver.edge.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.microsoft import EdgeChromiumDriverManager
# from selenium.webdriver.edge.service import Service as EdgeService
# from selenium.common.exceptions import TimeoutException, NoSuchElementException

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException



class NassauSpiderSpider(Spider):
    name = "nassau_spider"
    start_urls = ["https://i2f.uslandrecords.com/NY/Nassau/D/Default.aspx?AspxAutoDetectCookieSupport=1"]
    current_dt = datetime.now().strftime('%d%m%Y%H%M')

    custom_settings = {
        'CONCURRENT_REQUESTS': 3,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],

        'FEEDS': {
            f'Output/Nassau County Map Records {current_dt}.csv': {
                'format': 'csv',
                'fields': ['File Date', 'Title', 'Type Desc', 'Doc', 'Image']
            }
        }
    }
    post_headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://i2f.uslandrecords.com',
        'Pragma': 'no-cache',
        'Referer': 'https://i2f.uslandrecords.com/NY/Nassau/D/Default.aspx?AspxAutoDetectCookieSupport=1',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'X-MicrosoftAjax': 'Delta=true',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    post_cookies = {
        'NY/Nassau_DetailsViewMode': 'True',
        'NY/Nassau_DontAllowPopupTips': 'False',
        'NY/Nassau_DoNotShowSearchCriteria': 'False',
        'NY/Nassau_DoNotShowPrintChooseCriteria': 'True',
        'NY/Nassau_DoNotShowOrderChooseCriteria': 'True',
        'NY/Nassau_AutoDownloadExtraPage': '0',
        'NY/Nassau_IsImageUndock': 'False',
        'NY/Nassau_GroupName': 'Maps',
        'NY/Nassau_ModelName': 'Maps Recorded Date Search',
        'ASP.NET_SessionId': 'cd3121jo3whce3hqsf5indux',
    }

    def __init__(self):
        super().__init__()
        self.current_year = ''
        self.search_from_date = ''
        self.search_to_date = ''
        self.total_scraped_count = 0
        self.current_year_scraped_count = 0
        self.current_year_total_results = ''

        self.years = self.get_yearly_search_combinations()

        # Selenium Driver
        self.homepage_url = None
        self.driver = None

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'Script Started at "{self.script_starting_datetime}"\n')

        # Images Folder
        self.images_folder = 'Output/Image'
        # Check if images_folder exists, create if not
        if not os.path.exists(self.images_folder):
            os.makedirs(self.images_folder)

    def start_requests(self):
        yield Request(url=self.start_urls[0], callback=self.parse, meta={'handle_httpstatus_all': True})

    def parse(self, response, **kwargs):
        if 'Object moved' in response.text:
            url = response.css('a::attr(href)').get('')
            if url:
                self.homepage_url = f'https://i2f.uslandrecords.com{url}'
        else:
            self.homepage_url = self.homepage_url[0]

    def parse_home_page(self, response):
        try:
            cookies = self.get_cookies()
            if not cookies.get('ASP.NET_SessionId', ''):
                self.write_logs('Session id not updated')
                return

            self.post_cookies['ASP.NET_SessionId'] = cookies.get('ASP.NET_SessionId', '')
            self.post_headers['Referer'] = self.driver.current_url
            self.homepage_url = self.driver.current_url

            yield from self.parse_request_page_results(page_source=self.driver.page_source)

            selenium_page_html = Selector(text=self.driver.page_source)
            next_page_value = selenium_page_html.css('.PagerNextPrevButton:contains("Next") ::attr(href)').re_first(
                r"Back\('(.*?)',''\)")

            # Pagination
            # while next_page_value:
            #     next_page_reqs = requests.post(self.homepage_url,
            #                                    cookies=self.post_cookies, headers=self.post_headers,
            #                                    data=self.get_form_data(next_page=next_page_value))
            #     if next_page_reqs.status_code == 200:
            #         yield from self.parse_request_page_results(page_source=next_page_reqs.text)
            #         next_page_html = Selector(text=next_page_reqs.text)
            #         next_page_value = next_page_html.css('.PagerNextPrevButton:contains("Next") ::attr(href)').re_first(
            #             r"Back\('(.*?)',''\)")
            #     else:
            #         print(f'Failed to load next page: {next_page_reqs.status_code}')
            #         break

        except Exception as e:
            self.write_logs(f'Error parsing home page: {e}')

    def get_form_data(self, view_img=None, date_id=None, tab_image=None, next_page=None):
        try:
            data_dict = {
                'ScriptManager1': 'DocList1$UpdatePanel|DocList1$GridView_Document$ctl02$ButtonRow_File Date_0',
                'ScriptManager1_HiddenField': ';;AjaxControlToolkit, Version=3.5.40412.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:1547e793-5b7e-48fe-8490-03a375b13a33:effe2a26;;;AjaxControlToolkit, Version=3.5.40412.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:1547e793-5b7e-48fe-8490-03a375b13a33:475a4ef5:5546a2b:497ef277:a43b07eb:d2e10b12:37e2e5c9:5a682656:1d3ed089:f9029856:d1a1d569:addc6819:c7029a2:e9e598a9;',
                'Navigator1$SearchOptions1$DocImagesCheck': 'on',
                'Navigator1$SearchOptions1$SavePrintCriteriaCheck': 'on',
                'Navigator1$SearchOptions1$SaveOrderCriteriaCheck': 'on',
                'SearchCriteriaOffice1$DDL_OfficeName': 'Maps',
                'SearchCriteriaName1$DDL_SearchName': 'Maps Recorded Date Search',
                'SearchFormEx1$DRACSTextBox_DateFrom': '1/1/1900',
                'SearchFormEx1$DRACSTextBox_DateTo': '12/31/1910',
                'SearchFormEx1$ACSDropDownList_DocumentType': '-2',
                'ImageViewer1$ScrollPos': '',
                'ImageViewer1$ScrollPosChange': '',
                'ImageViewer1$_imgContainerWidth': '0',
                'ImageViewer1$_imgContainerHeight': '0',
                'ImageViewer1$isImageViewerVisible': 'true',
                'ImageViewer1$hdnWidgetSize': '',
                'ImageViewer1$DragResizeExtender_ClientState': '',
                'CertificateViewer1$ScrollPos': '',
                'CertificateViewer1$ScrollPosChange': '',
                'CertificateViewer1$_imgContainerWidth': '0',
                'CertificateViewer1$_imgContainerHeight': '0',
                'CertificateViewer1$isImageViewerVisible': 'true',
                'CertificateViewer1$hdnWidgetSize': '',
                'CertificateViewer1$DragResizeExtender_ClientState': '',
                'PTAXViewer1$ScrollPos': '',
                'PTAXViewer1$ScrollPosChange': '',
                'PTAXViewer1$_imgContainerWidth': '0',
                'PTAXViewer1$_imgContainerHeight': '0',
                'PTAXViewer1$isImageViewerVisible': 'true',
                'PTAXViewer1$hdnWidgetSize': '',
                'PTAXViewer1$DragResizeExtender_ClientState': '',
                'DocList1$ctl12': '',
                'DocList1$ctl14': '0',
                'RefinementCtrl1$ctl01': '',
                'RefinementCtrl1$ctl03': '',
                'NameList1$ScrollPos': '',
                'NameList1$ScrollPosChange': '',
                'NameList1$_SortExpression': '',
                'NameList1$ctl03': '',
                'NameList1$ctl05': '',
                'DocDetails1$PageSize': '',
                'DocDetails1$PageIndex': '',
                'DocDetails1$SortExpression': '',
                'BasketCtrl1$ctl01': '',
                'BasketCtrl1$ctl03': '',
                'OrderList1$ctl01': '',
                'OrderList1$ctl03': '',
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': '',
                '__ASYNCPOST': 'true'
            }

            if date_id and not view_img:
                data_dict['ScriptManager1'] = f'DocList1$UpdatePanel|{date_id}'
                data_dict['__EVENTTARGET'] = date_id
            elif not date_id and view_img:
                data_dict['ScriptManager1'] = f'DocList1$UpdatePanel|{view_img}'
                data_dict[view_img] = ''
            elif tab_image:
                data_dict['ScriptManager1'] = 'TabController1$UpdatePanel1|TabController1$ImageViewertabitem'
                data_dict['__EVENTTARGET'] = 'TabController1$ImageViewertabitem'
            elif next_page:
                data_dict['ScriptManager1'] = f'DocList1$UpdatePanel|{next_page}'
                data_dict['__EVENTTARGET'] = next_page

            elif not date_id and not view_img and not tab_image and not next_page:
                # this empty form data for seccond request for image
                print("No parameters provided, returning None.")
                return None

            formdata = urllib.parse.urlencode(data_dict)
            return formdata
        except Exception as e:
            self.write_logs(f'Form Data Error: {e}')
            return None

    def get_yearly_search_combinations(self):
        start_year = 1900
        end_year = 2024
        gap = 5

        year_combinations = []

        while start_year <= end_year:
            # Calculate the end year for the current range
            range_end_year = min(start_year + gap - 1, end_year)

            # Define the start and end dates for this range
            year_start_date = f"1/01/{start_year}"
            year_end_date = f"12/31/{range_end_year}"

            # Create the current year range string
            current_year = f"{start_year}-{range_end_year}"

            year_combinations.append({
                'start_date': year_start_date,
                'end_date': year_end_date,
                'year_range': current_year
            })

            # Move to the next range
            start_year += gap

        return year_combinations

    def get_plat_name(self, date_id, view_img, file_date):
        try:
            self.post_cookies.update({'NY/Nassau_IsImageUndock': 'False'})
            session = requests.session()
            session_req = session.post(self.homepage_url,
                                       cookies=self.post_cookies, headers=self.post_headers,
                                       data=self.get_form_data(date_id=date_id))

            html = Selector(text=session_req.text)

            file_date_formatted = '/'.join(file_date.split('/')[:-1])

            # Extract the value from the matching row
            texts = [tr.css('a::text').get('').strip() for tr in html.css('#DocDetails1_GridView_GrantorGrantee tr')]
            plat_desc = next((text for text in texts if file_date_formatted in text.replace('0', '')), None)
            if not plat_desc:
                plat_desc = ''.join(
                    html.css('#DocDetails1_GridView_GrantorGrantee tr:contains("Plat Desc") a::text').getall()[
                    -1:]).strip()
            if not plat_desc:
                plat_desc = file_date

            image = self.get_image(view_img, date_id, plat_desc)
            return plat_desc, image

        except Exception as e:
            self.write_logs(f'Error getting plat name for date_id {date_id}: {e}')
            return '', ''

    def get_image(self, view_img, date_id, plat_desc):
        try:
            # first Homepage Post request then next Request for image Url
            browser_url = f"{''.join(self.homepage_url.split('Default')[0:1])}ImageViewerEx.aspx"
            urls = [
                (self.homepage_url, 'POST'),
                (browser_url, 'GET'),
            ]

            data_params = [
                self.get_form_data(view_img=view_img),
                self.get_form_data()
            ]

            response = None
            for i, (url, request_type) in enumerate(urls):
                response = self.make_request(url, request_type, data_params[i])
                if 'preInit' in response.text:
                    break

            if response and 'preInit' in response.text:
                html = Selector(text=response.text)
                try:
                    preinit_value = html.css('script:contains("preInit") ::text').re_first(
                        r"preInit\('(.*?)','ImageViewer1_docImage'")
                except Exception as e:
                    self.write_logs(f"Error using css selector with re_first: {e}")
                    try:
                        preinit_script = html.css('script:contains("preInit") ::text').get('')
                        preinit_value = re.search(r"preInit\('([^']+)'", preinit_script).group(1)
                    except AttributeError as ae:
                        self.write_logs(
                            f"Year {self.current_year} Date {date_id} Plat Desc {plat_desc} Error using regex search: {ae}")
                        preinit_value = None

                if preinit_value:
                    img_url = f"{''.join(self.homepage_url.split('Default')[0:1])}{preinit_value}&CNTWIDTH=1000&CNTHEIGHT=750&FITTYPE=Height&ZOOM=3"
                    # Download the image
                    img_req = requests.get(img_url, cookies=self.post_cookies, headers=self.post_headers)

                    # Check if the request was successful
                    if img_req.status_code == 200:
                        return img_req
                    else:
                        self.write_logs(
                            f"Year {self.current_year} Date {date_id} Plat Desc {plat_desc} Failed to download image. Status code: {img_req.status_code}")
                else:
                    self.write_logs(
                        f"Year {self.current_year} Date {date_id} Plat Desc {plat_desc}  No valid Image value found.")
            else:
                self.write_logs(
                    f"Year {self.current_year} Date {date_id} Plat Desc {plat_desc} None of the requests returned the expected content.")

        except Exception as e:
            self.write_logs(f'Error getting image for date_id {date_id}: {e}')

    def write_logs(self, log_msg):

        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def make_request(self, url, request_type, data):
        if request_type == 'POST':
            response = requests.post(url, cookies=self.post_cookies, headers=self.post_headers, data=data)
        elif request_type == 'GET':
            self.post_headers[
                'Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
            response = requests.get(url, cookies=self.post_cookies, headers=self.post_headers, data=data)
        else:
            raise ValueError("Invalid request type. Use 'POST' or 'GET'.")

        return response

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(NassauSpiderSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if self.current_year:
            self.write_logs(f'Year: {self.current_year} Total Record Found {self.current_year_total_results}')
            self.write_logs(f'Year: {self.current_year} Total Record scraped {self.current_year_scraped_count}')

            self.current_year = ''
            self.current_year_total_results = ''
            self.current_year_scraped_count = 0

            # Clear cookies before quitting the driver
            if self.driver:
                self.driver.delete_all_cookies()
                # self.driver.close()
                self.driver.quit()

        self.write_logs('\n')
        # self.write_logs('#' * 60)
        self.write_logs(f'Search Combinations Left to Search: {len(self.years)}')

        if self.years:
            year = self.years.pop(0)
            self.current_year = year.get('year_range', '')
            self.search_from_date = year.get('start_date', '')
            self.search_to_date = year.get('end_date', '')

            self.write_logs(f'Year: {self.current_year} started for scraping')

            req = Request(url='https://quotes.toscrape.com/',
                          callback=self.parse_home_page,
                          dont_filter=True,
                          meta={'handle_httpstatus_all': True})

            try:
                self.crawler.engine.crawl(req)  # For latest Python version
            except TypeError:
                self.crawler.engine.crawl(req, self)  # For old Python version < 10

    def get_cookies(self):
        try:
            # self.driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()))
            # Setup Edge options for incognito mode
            # edge_options = Options()
            # edge_options.add_argument("--incognito")
            #
            # # Initialize the Edge driver with options
            # self.driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()),
            #                              options=edge_options)

            # Chrome
            # Setup Chrome options for incognito mode
            chrome_options = Options()
            chrome_options.add_argument("--incognito")

            # Initialize the Chrome driver with options
            self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),
                                           options=chrome_options)

            # Open the website
            self.driver.get(self.homepage_url)
            print('HomePage Url :', self.homepage_url)

            # Wait until "Nassau County Land Records" text is present in the page source
            # WebDriverWait(self.driver, 100).until(EC.text_to_be_present_in_element((By.TAG_NAME, 'body'), 'Nassau County Land Records'))
            # WebDriverWait(self.driver, 10).until(EC.text_to_be_present_in_element((By.TAG_NAME, 'body'), 'Nassau'))
            # sleep(5)

            # Wait for the page to load completely
            WebDriverWait(self.driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            # Wait until the "Office" dropdown is present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "SearchCriteriaOffice1_DDL_OfficeName"))
            )
            sleep(1)

            # Wait until the "Search Type" dropdown is present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "SearchCriteriaName1_DDL_SearchName"))
            )
            sleep(1)

            # Select "Maps" from the "Office" dropdown
            office_dropdown = Select(self.driver.find_element(By.ID, "SearchCriteriaOffice1_DDL_OfficeName"))
            office_dropdown.select_by_visible_text("Maps")

            # Validate that "Maps" is selected
            WebDriverWait(self.driver, 10).until(
                EC.text_to_be_present_in_element_value((By.ID, "SearchCriteriaOffice1_DDL_OfficeName"), "Maps")
            )

            sleep(3)

            # Select "Recorded Date Search" from the "Search Type" dropdown
            search_type_dropdown = Select(self.driver.find_element(By.ID, "SearchCriteriaName1_DDL_SearchName"))
            search_type_dropdown.select_by_visible_text("Recorded Date Search")

            # Validate that "Recorded Date Search" is selected
            WebDriverWait(self.driver, 10).until(
                EC.text_to_be_present_in_element_value((By.ID, "SearchCriteriaName1_DDL_SearchName"),
                                                       "Recorded Date Search")
            )

            sleep(5)

            # Clear and set the "From Date" input
            try:
                # Try to find the input field by its ID
                from_date_input = self.driver.find_element(By.ID, "SearchFormEx1_DRACSTextBox_DateFrom")
                validation_condition = (By.ID, "SearchFormEx1_DRACSTextBox_DateFrom")
            except NoSuchElementException:
                # If the input field by ID is not found, find it by name within the specified td
                from_date_cell = self.driver.find_element(By.ID, "SearchFormEx1_DataRange_Cell21")
                from_date_input = from_date_cell.find_element(By.NAME, "SearchFormEx1$DRACSTextBox_DateFrom")
                validation_condition = (By.NAME, "SearchFormEx1$DRACSTextBox_DateFrom")

            from_date_input.clear()
            # from_date_input.send_keys("1/01/1900")
            from_date_input.send_keys(self.search_from_date)

            # Validate that the value is set correctly
            WebDriverWait(self.driver, 10).until(
                EC.text_to_be_present_in_element_value(validation_condition, self.search_from_date)
            )

            sleep(1)

            # Clear and set the "To Date" input
            try:
                # Try to find the input field by its ID
                to_date_input = self.driver.find_element(By.ID, "SearchFormEx1_DRACSTextBox_DateTo")
            except NoSuchElementException:
                # If not found, try to find the input field by its name attribute
                to_date_input = self.driver.find_element(By.NAME, "SearchFormEx1$DRACSTextBox_DateTo")

            to_date_input.clear()
            # to_date_input.send_keys("12/31/1910")
            to_date_input.send_keys(self.search_to_date)

            # Validate that the value is set correctly
            WebDriverWait(self.driver, 10).until(
                EC.text_to_be_present_in_element_value((By.ID, "SearchFormEx1_DRACSTextBox_DateTo"),
                                                       self.search_to_date)
            )

            sleep(1)

            # Wait until the "Search" button is present
            search_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "SearchFormEx1_btnSearch"))
            )

            # Click the "Search" button
            search_button.click()

            sleep(4)

            # Wait until the results are loaded and the page size selector is present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "DocList1_PageView100Btn"))
            )

            # Select the maximum number of results per page
            max_results_button = self.driver.find_element(By.ID, "DocList1_PageView100Btn")
            max_results_button.click()

            sleep(5)

            # Get the total number of results
            total_results_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "SearchInfo1_ACSLabelParam_SearchResultCount"))
            )
            self.current_year_total_results = total_results_element.text

            # Retrieve cookies
            cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}

            return cookies

        except TimeoutException as e:
            self.write_logs(f'{self.current_year} Year Error: A TimeoutException occurred: {e}')
            self.write_logs(f'{self.current_year} Year Error: The page or one of the elements took too long to load.')
        except NoSuchElementException as e:
            self.write_logs(f'{self.current_year} Year Error: A NoSuchElementException occurred: {e}')
            self.write_logs(f'{self.current_year} Year Error: An element was not found on the page.')
        except Exception as e:
            self.write_logs(f'{self.current_year} Year Error: An error occurred: {e}')

        return None

    def sanitize_filename(self, filename):
        # Replace invalid characters with underscores
        return re.sub(r'[\\/*?:"<>|]', '_', filename)

    def parse_request_page_results(self, page_source):
        html = Selector(text=page_source)
        records = html.css('.DataGridRow, .DataGridAlternatingRow')

        if not records:
            self.write_logs(f'No records found on the home page. Year {self.current_year}, Start Date : {self.search_from_date}, ENd Date :{self.search_to_date}')
            return

        for record in records[:1]:
            file_date = record.css("td a[id*='ButtonRow_File Date']::text").get('').strip()
            date_id = ''.join(record.css('td a::attr(href)').get('').split("'")[1:2])
            type_desc = record.css("td a[id*='ButtonRow_Type Desc.']::text").get('').strip()
            doc_number = record.css("td a[id*='ButtonRow_Doc. #']::text").get('').strip()
            rec_time = record.css("td a[id*='ButtonRow_Rec. Time']::text").get('').strip()
            num_pages = record.css("td a[id*='ButtonRow_# of Pgs']::text").get('').strip()
            view_img = record.css("input[name*='ImgBut']::attr(name)").get('').strip()
            add_to_basket = record.css("input[name*='AddToBskBut']::attr(name)").get('').strip()

            plat_desc, map_image = self.get_plat_name(date_id, view_img, file_date)

            sanitized_plat_desc = self.sanitize_filename(plat_desc)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            nanoseconds = int(time() * 1e9) % 1e9
            image_name = f'{sanitized_plat_desc}_{doc_number}_{timestamp}_{nanoseconds}.jpg'
            file_path = os.path.join(self.images_folder, image_name)

            item = OrderedDict()
            item['File Date'] = file_date
            item['Title'] = plat_desc
            item['Type Desc'] = type_desc
            item['Doc'] = doc_number
            item['Image'] = image_name
            yield item

            if plat_desc and map_image:
                try:
                    with open(file_path, 'wb') as f:
                        f.write(map_image.content)
                    print(f"Image downloaded successfully to: {file_path}")

                    self.total_scraped_count += 1
                    self.current_year_scraped_count += 1
                    print('Current_year_scraped_count', self.current_year_scraped_count)
                    print('total_scraped_count', self.total_scraped_count)

                except Exception as e:
                    self.write_logs(f'File {plat_desc} : File Date :{file_date} not written successfully. Error: {e}')
                    continue
        return

    def close(spider, reason):
        spider.driver.quit()
