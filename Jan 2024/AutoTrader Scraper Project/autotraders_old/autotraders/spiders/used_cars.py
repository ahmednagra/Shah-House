import json
from collections import OrderedDict
from datetime import datetime
from math import ceil
from urllib.parse import unquote

import requests
from scrapy import Request, Spider, Selector


class UsedCarsSpider(Spider):
    name = "used_cars"

    xlsx_headers = ['Title', 'Make', 'Model', 'Year', 'Vin No',
                    'Location', 'Condition', 'Price', 'Average Market Price',
                    'Status', 'Main Image', 'Options', 'Mileage', 'Trim', 'Color',
                    'Body Type', 'Fuel Type', 'Engine', 'Drive Train', 'City Fuel Economy',
                    'Highway Fuel Economy', 'Combined Fuel Economy', 'Dealer Name',
                    'Dealer City', 'Dealer State', 'Dealer Zip', 'Dealer Rating',
                    'Dealer Phone', 'URL']

    custom_settings = {
        'log_level': 'error',
        'CONCURRENT_REQUESTS': 4,
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEEDS': {
            f'output/Autotrader Used Cars Details.xlsx': {
                'format': 'xlsx',
                'fields': xlsx_headers,
            }
        },
    }

    headers_json = {
        'authority': 'www.autotrader.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'no-cache',
        # 'cookie': 'ATC_ID=d1c23504-e863-4fd0-9a92-5f590e54d9b3; pxa_id=ukuV4ROuvcmD4b37ygGBXx4Q; abc=ukuV4ROuvcmD4b37ygGBXx4Q; pxa_at=true; _scid=c409c562-af87-4d8d-b576-ca8d383bb79b; FPID=FPID2.2.pTOD21NIRaditemJiAvnzRaMp0ug34E5y0o205cxvfg%3D.1706938966; _4c_=%7B%22_4c_mc_%22%3A%221971c5af-fe6a-4be8-bff8-47c7a99abd3f%22%7D; _pin_unauth=dWlkPU5EQmtOalV3WkRFdFl6SmlOUzAwWm1ReExUbGpPR0l0TWpVd1pUQTFNR0ppWlRreA; _tt_enable_cookie=1; _ttp=mgx_XNdxrAImncHYkMfjma3FsD8; _aeaid=69b02b94-0cb5-4c0f-896b-dc575a5dd57c; aelastsite=0iPHe8nD06zpX%2Bi2hbqXkegqWDuGEl98NaIfPNXd2qCTBP6jn3W4zI6NZl3%2FPu8W; abc_3rd_party=ukuV4ROuvcmD4b37ygGBXx4Q; pbjs-unifiedid_cst=zix7LPQsHA%3D%3D; ATC_USER_ZIP=98402; ae-visitorId=12402038jfhhsu30gtqum4; cz-sid=b99db330-38ce-45b0-b9ff-ce874724b233; g_state={"i_p":1707802387318,"i_l":3}; ae-lastseen=1707227877641; OptanonAlertBoxClosed=2024-02-07T08:26:59.787Z; tredTemp=eyJhdHRyaWJ1dGlvbiI6eyJsZWFkX2xhbmRpbmdfcGFnZSI6InZkcCIsInNvdXJjZSI6IkRpcmVjdCIsImxlYWRfZGV2aWNlIjoiZGVza3RvcCIsImxlYWRfY3JlYXRpb25fc291cmNlIjoiV2ViIn19; pxa_ipv4=212.102.46.34; pxa_bc=1f9ac7ca-772d-4c1d-9559-b58660a53402; _sctr=1%7C1707505200000; _clck=ojm0ie%7C2%7Cfj5%7C0%7C1494; _scid_r=c409c562-af87-4d8d-b576-ca8d383bb79b; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Feb+10+2024+11%3A33%3A42+GMT%2B0500+(Pakistan+Standard+Time)&version=202308.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=7f112e90-e180-49ac-99a1-263048fcf0d1&interactionCount=2&landingPath=NotLandingPage&groups=3xOT%3A1%2C1xOT%3A1%2C2xOT%3A1%2C5xOT%3A1%2C4xOT%3A1&AwaitingReconsent=false&geolocation=US%3BMI; SSM_GFU=eyJ2ZXJzaW9uIjoyLCJndWlkIjoiNjViZGU3NTBjODNjYjUyMDgwMDAwMmE4Iiwic2lkIjoiUzIwMjQwMjEwMDYzMzEwOXJ1MkU3cDBEeHBHMyIsImxhc3Rfc2VlbiI6MTcwNzU0NjgzMX0=; SSM_UTC=Z3VpZDo6NjViZGU3NTBjODNjYjUyMDgwMDAwMmE4fHx8c291cmNlOjpnZnU=; SSM_UTC_LS=Z3VpZDo6NjViZGU3NTBjODNjYjUyMDgwMDAwMmE4fHx8c291cmNlOjpnZnU=; _tq_id.TV-7209362745-1.7cb4=1b00a9fd1807cd75.1706938977.0.1707546845..; _td=8795d243-8a2d-4410-9744-c547a8388f4b; __eoi=ID=d1620a25885448c3:T=1706938965:RT=1707549173:S=AA-AfjZUlSMvVCWAmWzQ8jDtVHx4; pixall_cookie_sync=false; ATC_USER_RADIUS=0; bm_sz=3F17B7E2A420EE4F14F612EB4D888979~YAAQH+/dF+36FIaNAQAAC3Q4nRY0HuaGrShO6HIZGWoqir0a1mOVkGX91GBq3dODwCg/+5ZGmRh9hwV8HKiFzosV96LfJLoFvbE4JgPTxWXa2TGLTbd4LZzNxXqJ4n0WeQylvF+EkWv3ERMQrdRM5gcw3VaaywbX9zVPaVKlDgCAhqswhYkwqFprZtpsMFd3eI6QhTz4+sEYYvkfH+nNd1rvby3gFLRXvJcpyNMxQg5WbD76usbOLDC5Ux05RoG1NJznAOf3PXGi2MWUxafu6Duj42pi+MYWlaHCX9hXBPGsphTN1PrejypPGf8cUTKwjT7hZiHDdT/ZIlIh5jzjdjrT/Kk/qL4mKxtMBWLJtCroBbQ+iHQJDwwkUg1IT6lqfbp+OVKKK6JW/ZSOrbwIekQ=~4539186~3228982; akaalb_at_alb=1707743471~op=~rv=84~m=~os=~id=9069a87c39d3d7234f4c9961b00981c4; ak_bmsc=C299F75092811CAB4A038508E01EBD60~000000000000000000000000000000~YAAQZu4uF95cUZCNAQAAeX1inRb14jcws/+9Y2ftKfHPaZFjShRq4oJYIJFrQsAFW6jnTrin6tvdFOJ5nIvhsR+CHHsZkVmupGwYS40o+yvU+t3SHvG9cN0zTmy7DK/tastaj+ZrwpBapdY21aGzKWVeyjRL5UM0DBIiFap9iX414MEGYDcgPwjL1/k7Nw6F3sZErMK7vC3PKHio4cojlCt9Sx2KpwtEMq8VKDqMgOyS6Xpvfs7JfKNUa3A6JhM87CXa+/91SrFJ0Rjk0rHqRmVBTfIpGY72p6btzWv9ZjyQg5EOWG/LjY5NmmErkbtG9f6JPr+4Y0XIOo6eFI5QthaTJ5LJqFf4QvYL0CC7GG+u608Y1htZAW0l5QEgSWQwjhABX/ZuRAQ3DwC12w==; search_lbcookie=tvFlqWpgsC4zGZrZE/Nhu4g4objMhPT1ksw9jaQd+rpoogWqqonrawN7I/33EtTEpayrfabs8dtG4Ow25m99+rC4u6M4EvKe566tbsAZlt1wDCutYrOVVAX++nmx; x-coxauto-aka-data=US|FL|32801|FL|||||800|1280|Windows NT|Chrome; _abck=9F6CCE60C847C5308AFA85F09F6ADFF7~0~YAAQRGgBF8swUYONAQAABhtlnQsNQ7ilcN9h0AjAaKDRfwwF8H+5u+fSK4AvwKot4lj6/hXI1TLacMo/SCg275pHUyFP6RlQryxVTXiy5DGHVPwefXpBQAYF2P6MJHZrUIj2xqp9YT1xRA7+29lhDvPl4yrqlqLQxKBN8KZZt4yoGB71PHi4thkvUN0NxJHuaRKyiIycL0jQP0D4noDfQ/yVwd9YFO1droHIxv4ao4AxvkekT5a00VMvdZGGNVjYY8eY1uyOwcGAHm7oj/ppFQqHsbaJvFA2zlBE7pvZmCmX6i9rUMLeHiZ34/91kgMnggYluxmjhrlIiP4pmi8Y+kDr5loJnjqYH9PjYTpYOiLgCH8nwu1pKLnDPMl49TES8UA=~-1~-1~-1; bm_sv=6BA597602B083D39A5EA59DD42B6F504~YAAQRGgBF8wwUYONAQAABhtlnRZZvZuSOs71nlb6Tv65WSYA3QWCIUkyNjcQFgNHATfYpRAX5JgQmgdqe/TTIz35n81K5A0cxJ7rFTPfpcf7NTgAsFNaVB38TPZf3jwo1Vcz0WPfFCtODXoQCno97cKM8f0Boi2KdPz6WwTXdb3+XveNr/92m9ZkbM46DnVx9xLWvYo/wPlqBgy03g4yUcfHci2+QTGDQ5TuIYn2VuHcIFGxYtKCMpoT0+Fuzp//z7IVHQ==~1',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    headers = {
        'authority': 'www.autotrader.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'no-cache',
        # 'cookie': 'ATC_ID=d1c23504-e863-4fd0-9a92-5f590e54d9b3; pxa_id=ukuV4ROuvcmD4b37ygGBXx4Q; abc=ukuV4ROuvcmD4b37ygGBXx4Q; pxa_at=true; _scid=c409c562-af87-4d8d-b576-ca8d383bb79b; FPID=FPID2.2.pTOD21NIRaditemJiAvnzRaMp0ug34E5y0o205cxvfg%3D.1706938966; _4c_=%7B%22_4c_mc_%22%3A%221971c5af-fe6a-4be8-bff8-47c7a99abd3f%22%7D; _pin_unauth=dWlkPU5EQmtOalV3WkRFdFl6SmlOUzAwWm1ReExUbGpPR0l0TWpVd1pUQTFNR0ppWlRreA; _tt_enable_cookie=1; _ttp=mgx_XNdxrAImncHYkMfjma3FsD8; _aeaid=69b02b94-0cb5-4c0f-896b-dc575a5dd57c; aelastsite=0iPHe8nD06zpX%2Bi2hbqXkegqWDuGEl98NaIfPNXd2qCTBP6jn3W4zI6NZl3%2FPu8W; abc_3rd_party=ukuV4ROuvcmD4b37ygGBXx4Q; pbjs-unifiedid_cst=zix7LPQsHA%3D%3D; ATC_USER_ZIP=98402; ae-visitorId=12402038jfhhsu30gtqum4; cz-sid=b99db330-38ce-45b0-b9ff-ce874724b233; g_state={"i_p":1707802387318,"i_l":3}; ae-lastseen=1707227877641; OptanonAlertBoxClosed=2024-02-07T08:26:59.787Z; tredTemp=eyJhdHRyaWJ1dGlvbiI6eyJsZWFkX2xhbmRpbmdfcGFnZSI6InZkcCIsInNvdXJjZSI6IkRpcmVjdCIsImxlYWRfZGV2aWNlIjoiZGVza3RvcCIsImxlYWRfY3JlYXRpb25fc291cmNlIjoiV2ViIn19; ATC_USER_RADIUS=50; pxa_ipv4=212.102.46.34; pxa_bc=1f9ac7ca-772d-4c1d-9559-b58660a53402; _sctr=1%7C1707505200000; _clck=ojm0ie%7C2%7Cfj5%7C0%7C1494; _scid_r=c409c562-af87-4d8d-b576-ca8d383bb79b; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Feb+10+2024+11%3A33%3A42+GMT%2B0500+(Pakistan+Standard+Time)&version=202308.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=7f112e90-e180-49ac-99a1-263048fcf0d1&interactionCount=2&landingPath=NotLandingPage&groups=3xOT%3A1%2C1xOT%3A1%2C2xOT%3A1%2C5xOT%3A1%2C4xOT%3A1&AwaitingReconsent=false&geolocation=US%3BMI; SSM_GFU=eyJ2ZXJzaW9uIjoyLCJndWlkIjoiNjViZGU3NTBjODNjYjUyMDgwMDAwMmE4Iiwic2lkIjoiUzIwMjQwMjEwMDYzMzEwOXJ1MkU3cDBEeHBHMyIsImxhc3Rfc2VlbiI6MTcwNzU0NjgzMX0=; SSM_UTC=Z3VpZDo6NjViZGU3NTBjODNjYjUyMDgwMDAwMmE4fHx8c291cmNlOjpnZnU=; SSM_UTC_LS=Z3VpZDo6NjViZGU3NTBjODNjYjUyMDgwMDAwMmE4fHx8c291cmNlOjpnZnU=; _tq_id.TV-7209362745-1.7cb4=1b00a9fd1807cd75.1706938977.0.1707546845..; _td=8795d243-8a2d-4410-9744-c547a8388f4b; __eoi=ID=d1620a25885448c3:T=1706938965:RT=1707549173:S=AA-AfjZUlSMvVCWAmWzQ8jDtVHx4; pixall_cookie_sync=false; bm_sz=968BB8E7DEBBA309633160DA12BA4340~YAAQjGrcF03XOXONAQAA2km+nBYxOGYJMo9mK06gPYj+vuhq5UzPXU0ywUQdWFTYQNzMXGoLY7pVkGjFKD1ICXnNnfTVeZ2HMtHoU5GCFCTx20S+yVCo6siIoFXXM6UKB6Skf8Bvz757NEEfkYkSuFBwcA3UMwrB8WlOZrEqruuQ6hggqUSKjn6GcztABWokzAU//5BlD+lFiIMcwQLxTtQsZCwkbgdbXEQ6ghYvAYE5K8FuQJEg9qd70sDX7CdYDZ+pcyBVQRkAXvMOcp9s16KOqLKaTMQeNxc2EDOKksm2cwjS9UqWndkpU5fdeJMXg8m3gVrcYKYsEq+MlvlKvQ==~4277560~3486261; search_lbcookie=dnXmIfh%2FVQwEmenBlawKe6NflTuMz7rK4hAAdUJjQxw6TL1KPDweFetsR4UomIwDUY20f%2FkaIcLlutIzK4XlpSrEd0JPikSKQbDIpUt7kkfOIpe76tgP%2F8K35c4A; _abck=9F6CCE60C847C5308AFA85F09F6ADFF7~0~YAAQjGrcFzfYOXONAQAABFe+nAs9q8cA7D1db77c5jw0iA76Jef8nDUm2iyVw1Kw0VhL9nK3XQx/VZ77n2e7yYz0Sk2my82S+bkbaFJp+SyY6nFKJbz2kQbCsbsby+WRi3JGX9PZhtK2F+lx6oE9H3gc6pbBv9y1SCMOiYbbNocj1ISNgVuO9suh/LRlARl9XxEaQPfIM37tmISwrd48t/G/2Jdke6iliTz5inJvgTitegZjL+chJSrxyw9CfVWHpHYOx4AkKFe7RWm62YTJ45Ewjwwb+yIK61w8IGdu9Usl5551q9qNza3pdCfIFEQT5eRBoYGiDlTsYMGCADGcVR1tOL52QsSGecHwWBV9VJkRB0oq/FOugRiRKrxWtswksnEv/KJikP6wwYWKnrHlicGCGrnQB+WTsa4grQ==~-1~-1~-1; x-coxauto-aka-data=US|MI|48340|MI|||||800|1280|Windows NT|Chrome; akaalb_at_alb=1707736942~op=AT_col_lb:AT_col-rp|~rv=92~m=AT_col-rp:0|~os=659c3ec2d171bf62456d07d58a4d53bf~id=b8fc396423803453ccc445c14af5c401; ak_bmsc=AF6E4032129520790FA38EC3D8116A21~000000000000000000000000000000~YAAQExkhF8vLlXKNAQAAaE/ynBaBbvepc6h0Hrj9If0/iSY8kXVSey+CQU/KL3uqyvxW1nWhkUHGCWAQcWZ2wQfSrqEnQ8HpqWAgwITAAbihc4IaijnNQW0/rsvkseor1GmdF2UVzfLS6JqKQRpf0ob5zr/bm4HTQ46XLOfohvMBh1oLQB+Dq1nkYW5Kn1c1MwPuIObNpQX+oxwG4D0vXa4cBdPiUByx8Xk2nvIN8UrgWYX1IqNPFotuyYQt//bCfcvv8BtYWV9B3JSDOTRz+se5uWwYkKvGNV6NAjGIrpMpmNx1kB7QSX9qtFpPCDFE28arW8jNxzVzFqgsXL0iv0QD+k35DiwbpfS0T0u0sNC6eaZZsPdgWNIjofGs1qJNnOqjjsYCj3qGxCY0cE8=',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.current_scraped_items = []
        self.all_makers = ['AMC', 'ACURA', 'ALFA', 'ASTON', 'AUDI', 'BMW', 'BENTL', 'BUGATTI', 'BUICK', 'CAD', 'CHEV',
                           'CHRY', 'DAEW', 'DATSUN', 'DELOREAN', 'DODGE', 'EAGLE', 'FIAT', 'FER', 'FISK', 'FORD',
                           'FREIGHT', 'GMC', 'GENESIS', 'GEO', 'AMGEN', 'HONDA', 'HYUND', 'INEOS', 'INFIN', 'ISU',
                           'JAG', 'JEEP', 'KARMA', 'KIA', 'LAM', 'ROV', 'LEXUS', 'LINC', 'LOTUS', 'LUCID', 'MAZDA',
                           'MINI', 'MAS', 'MAYBACH', 'MCLAREN', 'MB', 'MERC', 'MIT', 'NISSAN', 'OLDS', 'PLYM',
                           'POLESTAR', 'PONT', 'POR', 'RAM', 'RIVIAN', 'RR', 'SRT', 'SAAB', 'SATURN', 'SCION', 'SUB',
                           'SUZUKI', 'TESLA', 'TOYOTA', 'VINFAST', 'VOLKS', 'VOLVO', 'YUGO', 'SMART']

        self.current_products_scraped = 0
        self.proxy_key = '69407ad1-67b8-4a4f-8083-137167f3b908'
        self.logs_info_container = []

    def start_requests(self):
        yield Request(url='https://www.autotrader.com/cars-for-sale', callback=self.parse, headers=self.headers)

    def parse(self, response, **kwargs):
        data = {}
        try:
            data = json.loads(''.join(response.css('script:contains("BONNET_DATA") ::text').re(r'({.*})')))
        except Exception as e:
            try:
                res = requests.get(unquote(response.url).split('url=')[1])
                if res.ok:
                    sel = Selector(text=res.text)
                    data = json.loads(''.join(sel.css('script:contains("BONNET_DATA") ::text').re(r'({.*})')))
                else:
                    print(f'Error from pagination: {res.status_code}')
                    return
            except Exception as e:
                print(f'Error from pagination: {e}')
                log_msg = f'\n\n{datetime.now()} -> Function Name : Parse and error :{e}'
                self.logs_info_container.append(log_msg)
                return

        vehicle_makers_dict = data.get('props', {}).get('homepage', {}).get('vehicleMakes', [])
        if not vehicle_makers_dict:
            vehicle_makers_dict = data.get('props', {}).get('landingPage', {}).get('vehicleMakes', [])

        vehicle_codes = [v.get('makeCode') for v in vehicle_makers_dict]
        if not vehicle_codes:
            vehicle_makers = self.all_makers
        else:
            vehicle_makers = vehicle_codes

        for vehicle_maker in vehicle_makers:
            url = f'https://www.autotrader.com/collections/ccServices/rest/ccs/models?makeCode={vehicle_maker}&pixallId=ukuV4ROuvcmD4b37ygGBXx4Q'
            yield Request(url=url, callback=self.parse_make_pagination, headers=self.headers_json,
                          meta={'vehicle_code': vehicle_maker})

    def parse_make_pagination(self, response):
        try:
            data = self.extract_json_data(response)
            maker_models = [model.get('value') for model in data if model.get('value') is not None]
            # print(f"Maker = {response.meta.get('vehicle_code', '')}, Models List = {maker_models}")
            for model in maker_models:
                maker = response.meta.get('vehicle_code', '')
                url = f'https://www.autotrader.com/rest/lsc/listing/?listingType=USED&makeCode={maker}&modelCode={model}&newSearch=false&ignoreEcommLink=false&channel=ATC&numRecords=2500'
                yield Request(url=url, callback=self.parse_model_indexing, headers=self.headers_json)
        except Exception as e:
            print(f'Error in parse_make: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : parse_make_pagination and error :{e}'
            self.logs_info_container.append(log_msg)

    def parse_model_indexing(self, response):
        try:
            data = self.extract_json_data(response)
            total_properties = data.get('totalResultCount', 0)

            if total_properties == 0:
                print(f'No product found : {response.url}')
                return

            if total_properties <= 2500:
                yield from self.parse_product_detail(response)
            else:
                yield from self.parse_milage_pagination(response)
        except Exception as e:
            print(f'Error in parse_index_page: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : parse_model_indexing and error :{e}'
            self.logs_info_container.append(log_msg)

    def parse_milage_pagination(self, response):
        mileage_list = ['15000', '30000', '45000', '60000', '75000', '100000', '150000', '200000', '200001']
        for value in mileage_list:
            url = f"{unquote(response.url).split('url=')[1]}&mileage={value}"
            yield Request(url=url, callback=self.parse_milage_indexing, dont_filter=True, headers=self.headers_json)

    def parse_milage_indexing(self, response):
        try:
            data = self.extract_json_data(response)
            total_properties = data.get('totalResultCount', 0)
            print(f'parse_milage_indexing :Total Product Found : ', total_properties)

            if total_properties == 0:
                print(f'No product found : ', response.url)
                return

            if total_properties <= 2500:
                yield from self.parse_product_detail(response)
            else:
                yield from self.parse_price_pagination(response)
        except Exception as e:
            print(f'Error in parse_price_pagination: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : parse_milage_indexing and error :{e}'
            self.logs_info_container.append(log_msg)

    def parse_price_pagination(self, response):
        try:
            data = self.extract_json_data(response)
            total_properties = data.get('totalResultCount', 0)
            print(f'parse_price_pagination :Total Product Found : {total_properties}')

            if total_properties == 0:
                print(f'No product found : {response.url}')
                return

            total_products = data.get('totalResultCount', 0)
            total_pages = ceil(total_products / 2500)

            filters = self.get_price_filters(total_pages)
            for min_price, max_price in filters:
                url = f'{response.url}&minPrice={min_price}&maxPrice={max_price}'
                yield Request(url=url, callback=self.parse_price_indexing, headers=self.headers_json)
        except Exception as e:
            print(f'Error in pagination: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : parse_price_pagination and error :{e}'
            self.logs_info_container.append(log_msg)

    def parse_price_indexing(self, response):
        try:
            data = self.extract_json_data(response)
            total_properties = data.get('totalResultCount', 0)
            print(f'parse_price_indexing :Total Product Found : {total_properties}')

            if total_properties == 0:
                print(f'No product found : {response.url}')
                return

            if total_properties <= 2500:
                yield from self.parse_product_detail(response)
            else:
                # yield from self.parse_product_detail(response)  # first 2500 items yields
                yield from self.regenerate_request(response)
                return

        except Exception as e:
            print(f'Error in pagination: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : parse_price_indexing and error :{e}'
            self.logs_info_container.append(log_msg)

    def parse_product_detail(self, response):
        data = self.extract_json_data(response)
        if data.get('totalResultCount', 0) == 0:
            return

        products = data.get('listings', [])
        if not products:
            return

        for product in products:
            # Extract basic information from the JSON data
            p_id = product.get('id', '')
            if p_id in self.current_scraped_items:
                # print(f'{p_id} already Exist')
                continue

            make = product.get('make', {}).get('name', '')
            model = product.get('model', {}).get('name', '')
            year = product.get('year', 0)
            condition = product.get('listingTypes', {})
            if condition:
                condition = product.get('listingTypes', {})[0].get('name', '') or product.get('listingType', '')
            price = product.get('pricingDetail', {}).get('salePrice', 0)
            avg_market_price = product.get('pricingDetail', {}).get('kbbFppAmount', 0.0)

            # Create an ordered dictionary to store item data
            item = OrderedDict()
            item['Title'] = product.get('title', '').replace(str(year), '').replace(condition, '').strip()
            item['Make'] = make
            item['Model'] = model.replace(str(make), '').replace(str(year), '').strip()
            item['Year'] = year if year else ''
            item['Vin No'] = product.get('vin', '')

            # Populate additional fields
            item['Location'] = ''
            item['Condition'] = condition
            item['Price'] = price if price else ''
            item['Average Market Price'] = avg_market_price if avg_market_price else ''
            item['Status'] = product.get('pricingDetail', {}).get('dealIndicator', '')
            item['Main Image'] = ''.join(
                [img.get('src', '') for img in product.get('images', {}).get('sources', [{}])][:1])
            item['Options'] = self.get_options(product)
            milage = product.get('mileage', {}).get('value', '').strip()
            item['Mileage'] = milage if milage else ''
            item['Trim'] = product.get('trim', {}).get('code', '')

            # Extract specific specifications
            body_styles = product.get('bodyStyles', [])
            if body_styles:
                body_type = body_styles[0].get('code', '')
            else:
                body_type = ''

            item['Color'] = product.get('color', {}).get('exteriorColor', '')
            item['Body Type'] = body_type
            item['Drive Train'] = product.get('driveType', {}).get('description', '')

            item['Fuel Type'] = product.get('fuelType', {}).get('name', '')
            item['Engine'] = product.get('engine', {}).get('name', '')

            city_fuel = product.get('mpgCity', 0)
            highway_fuel = product.get('mpgHighway', 0)
            item['City Fuel Economy'] = city_fuel if city_fuel else ''
            item['Highway Fuel Economy'] = highway_fuel if highway_fuel else ''
            item['Combined Fuel Economy'] = ''

            # Extract dealer information
            dealer_address = product.get('owner', {}).get('location', {}).get('address', {})
            dealer_rating = product.get('owner', {}).get('rating', {}).get('value', 0.0)
            item['Dealer Name'] = product.get('owner', {}).get('name', '')
            item['Dealer City'] = dealer_address.get('city', '').strip()
            item['Dealer State'] = dealer_address.get('state', '')
            item['Dealer Zip'] = dealer_address.get('zip', '')
            item['Dealer Rating'] = dealer_rating if dealer_rating else ''
            item['Dealer Phone'] = product.get('owner', {}).get('phone', {}).get('value', '')
            item['URL'] = f'https://www.autotrader.com/cars-for-sale/vehicle/{p_id}'

            self.current_products_scraped += 1
            print('Current Scraped Items Counter :', self.current_products_scraped)
            self.current_scraped_items.append(p_id)
            yield item

    def get_options(self, product):
        specifications = product.get('specifications', {})
        options_string = ""

        for key, value in specifications.items():
            label = value.get('label', '')
            option_value = value.get('value', '')
            option_string = f"{label}: {option_value}"
            options_string += f"{key.title()} = [{option_string}]\n"

        return options_string.strip()

    def regenerate_request(self, response):
        try:
            url = unquote(response.url).split('url=')[1]
            min_price = int(url.split('minPrice=')[1].split('&')[0])
            max_price = int(url.split('maxPrice=')[1])
            mid_price = (max_price + min_price) // 2
            filters = [(min_price, mid_price), (mid_price, max_price)]
            new_urls = [f"{url.split('minPrice=')[0]}minPrice={min_price}&maxPrice={max_price}" for min_price, max_price
                        in filters]
            for new_url in new_urls:
                yield Request(url=new_url, callback=self.parse_product_detail,
                              meta={'min_price': min_price, 'max_price': max_price}, headers=self.headers_json)
        except Exception as e:
            print(f'Error in regenerate_request: {e}')
            log_msg = f'\n\n{datetime.now()} -> Function Name : regenerate_request and error :{e}'
            self.logs_info_container.append(log_msg)

    def get_price_filters(self, total_pages):
        if total_pages < 5:
            filters = [(0, 20000), (20000, 40000), (40000, 60000), (60000, 80000),
                       (80000, 100000), (100000, 2500000), (2500000, 5000000),
                       (5000000, 10000000), (10000000, 15000000), (15000000, 30000000),
                       (30000000, 90000000), (90000000, 250000000),
                       (250000000, 500000000), (500000000, 700000000), (700000000, 800000000),
                       (800000000, 1000000000), (1000000000, 70000000000)]
        else:
            filters = [
                (0, 10000), (10000, 20000), (20000, 30000), (30000, 40000), (40000, 50000), (50000, 60000),
                (60000, 70000), (70000, 80000), (80000, 900000), (90000, 100000), (100000, 150000),
                (150000, 200000),
                (200000, 250000), (250000, 350000), (350000, 450000), (450000, 600000),
                (600000, 750000), (750000, 1000000), (1000000, 2000000), (2000000, 2500000),
                (2500000, 3000000), (3000000, 4500000), (4500000, 6000000), (6000000, 9000000),
                (9000000, 15000000), (15000000, 300000000), (300000000, 800000000)]
        return filters

    def extract_json_data(self, response):
        try:
            data = response.json()
        except Exception as e:
            try:
                res = requests.get(unquote(response.url).split('url=')[1])
                if res.ok:
                    data = res.json()
                else:
                    print(f'Error from extract_json_data: {res.status_code}')
                    data = {}
            except Exception as e:
                print(f'Error from extract_json_data: {e}')
                log_msg = f'\n\n{datetime.now()} ->  error :{e}'
                self.logs_info_container.append(log_msg)
                data = {}

        return data

    def close(spider, reason):
        try:
            with open(f"ERRORS_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt", mode='a') as log_file:
                for info_message in spider.logs_info_container:
                    log_file.write(f"{info_message}\n")
        except Exception as e:
            print(f"Error writing logs to file: {e}")
