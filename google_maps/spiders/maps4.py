# -*- coding: utf-8 -*-
import io
import re
import csv
import pkgutil

import scrapy
import usaddress

from scrapy import Request
from urllib.parse import quote_plus


class MapsSpider(scrapy.Spider):
    name = 'google_business4' 
    search_keyword = '{keyword}'
    base_url = 'https://www.google.ca/'
    start_urls = ["https://quotes.toscrape.com/"]
    business_urls = []
    custom_settings = {
        'FEED_URI': 'outputs/Output/business_data.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
    new_listings_url_t = 'https://www.google.com/localservices/prolist?ssta=1&src=2&q={q}&lci={page}'
    new_details_url_t = 'https://www.google.com/localservices/prolist?g2lbs=AP8S6ENgyDKzVDV4oBkqNJyZonhEwT_VJ6_XyhCY8jgI2NcumLHJ7mfebZa8Yvjyr_RwoUDwlSwZt5ofLQk3D079b7a0tYFMAl-OvnNjzh2HzyjZNDGO0bloXZTJ8ttkCFt5rwXuqt_u&hl=en-PK&gl=pk&ssta=1&oq={q}&src=2&sa=X&scp=CgASABoAKgA%3D&q={q}&ved=2ahUKEwji7NSKjZiAAxUfTEECHdJnDF8QjdcJegQIABAF&slp=MgBAAVIECAIgAIgBAJoBBgoCFxkQAA%3D%3D&spp={id}'

    listings_url_t = 'https://www.google.com/search?sxsrf=ACYBGNS1OuAlrwXrWvHCe01W6jx80oL9jA:1581870852554&q={q}&npsic=0&rflfq=1&rlha=0&rllag=-33868535,151194512,2415&tbm=lcl&ved=2ahUKEwiN1fyRwNbnAhUHVBUIHdOxBdIQjGp6BAgLEFk'
    RETRY_HTTP_CODES = [400, 403, 407, 408, 429, 500, 502, 503, 504, 405, 503, 520]
    handle_httpstatus_list = [400, 401, 402, 403, 404, 405, 406, 407, 409, 500, 501, 502, 503, 504,
                              505, 506, 507, 509]

    def parse(self, response, *args):
        keywords = self.get_input()
        for keyword in keywords:
            search_keyword = f"{keyword.get('search_keyword')}"
            query = self.search_keyword.format(keyword=search_keyword)
            url = self.new_listings_url_t.format(q=quote_plus(query), page=0)
            meta = {'keyword': search_keyword, 'start': 0,
                    'query': query,
                    'data': keyword}
            yield Request(url=url, callback=self.parse_new_data, meta=meta, dont_filter=True)

    def parse_new_data(self, response):
        if response.css('div[jsname="AECrIc"]'):
            for listing_selector in response.css('div[jscontroller="xkZ6Lb"]'):
                listing_id = listing_selector.css('::attr(data-profile-url-path)').get('').replace(
                    '/localservices/profile?spp=', '')
                in_type = listing_selector.css('span.hGz87c::text').get('').strip()
                response.meta['type'] = in_type
                response.meta['id'] = listing_id
                details_url = self.new_details_url_t.format(
                    q=quote_plus(response.meta['keyword']),
                    id=listing_id)
                Name = listing_selector.css('.xYjf2e::text').get('').strip()
                Address = listing_selector.css('.hGz87c span::text').getall()[-1] if listing_selector.css(
                    '.hGz87c span::text').getall() else ''
                if f"{Name} {Address}" not in self.business_urls:
                    self.business_urls.append(f"{Name} {Address}")
                    yield scrapy.Request(url=details_url, callback=self.parse_new_details,
                                         meta=response.meta, dont_filter=True)

            keyword = response.meta['keyword']
            start = response.meta['start'] + 10
            query = response.meta['query']
            url = self.new_listings_url_t.format(q=quote_plus(query), page=start)
            meta = {'keyword': keyword, 'start': start, 'query': query, 'data': response.meta['data']}
            if response.css('div[jscontroller="xkZ6Lb"]'):
                yield Request(url=url, callback=self.parse_new_data, meta=meta, dont_filter=True)

    def parse_new_details(self, response):
        item = dict()
        item['search_keyword'] = response.meta['keyword']
        item['Company Name'] = response.css('div.tZPcob::text').get('').strip()
        item['Company Phone'] = response.css('div.eigqqc::text').get('').strip().replace(' ', '').replace('-',
                                                                                                          '').replace(
            '+', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        item['Rating'] = response.css('.pNFZHb div.rGaJuf::text').get('')
        item['Review'] = response.css('.pNFZHb div.leIgTe::text').get('').replace('(', '').replace(')', '')
        item['Service Areas'] = ' '.join(response.css('.oR9cEb *::text').getall())
        item['Company Website'] = self.get_url(response.css('a.iPF7ob::attr(href)').get('').strip())
        item['Company Address'] = response.css('div.fccl3c span::text').get('').strip()
        item.update(self.get_address(response.css('div.fccl3c span::text').get('').strip()))
        if not item.get('Company Address') or 'United States' in item.get('Company Address'):
            yield item
        elif item.get('address1') and not item.get('country'):
            yield item
        else:
            yield {}

    def get_address(self, address):
        address1, city, state, zip_code, country = [], [], [], [], []
        address_parts = usaddress.parse(address)
        for value, attr in address_parts:
            if attr == 'PlaceName':
                city.append(value)
            elif attr == 'StateName':
                state.append(value)
            elif attr == 'ZipCode':
                zip_code.append(value)
            elif attr == 'CountryName':
                country.append(value)
            else:
                address1.append(value)
        zip_code = '-'.join(zip_code)
        if '-' in zip_code:
            zip_code, zip_plus = zip_code.split('-')[0], zip_code.split('-')[-1]
        else:
            zip_code, zip_plus = zip_code, ''

        address1 = f"{' '.join(address1)}".strip().strip(',')
        city = f"{' '.join(city)}".strip().strip(',')
        state = f"{' '.join(state).strip()}".strip().strip(',')
        country = ' '.join(country).strip()

        address_item = {
            'address1': address1,
            'city': city,
            'state': state,
            'zip': zip_code.strip().strip(','),
            'zip_plus': zip_plus,
            'country': country
        }
        return address_item

    def get_url(self, string):
        pattern = r'url=(.*?)&'

        # Use re.search to find the first match
        match = re.search(pattern, string)

        # Check if a match was found
        if match:
            # Extract the URL from the match
            url = match.group(1)
            return url
        else:
            return ''

    def get_input(self):
        csv_data = pkgutil.get_data("google_maps", "input/search_queries4.csv")
        csv_string = csv_data.decode('utf-8')
        csv_file = io.StringIO(csv_string)
        return list(csv.DictReader(csv_file))
