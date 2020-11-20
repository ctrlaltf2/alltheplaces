import json
import re
import scrapy
from six.moves.urllib.parse import urlencode
from locations.items import GeojsonPointItem

class WawaSpider(scrapy.Spider):
    name = 'wawa'
    item_attributes = { 'brand': "Wawa" }
    download_delay = 4
    allowed_domains = ['www.wawa.com']

    def start_requests(self):
        """ Initial request: get the list of valid store numbers through a sitemap xml """
        yield scrapy.Request('https://www.wawa.com/search-sitemaps/store-sitemap',
                             callback=self.process_sitemap,
                             method='GET',
                             headers = { 'Host': 'www.wawa.com' },
                             cookies = {'reese84': '3:dxpwQ5yByy9PqWsb714GBA==:hAwjbzbzIKd6d49Ipk9bvsE/pRsmNkAjX1OGKPkepY+omEu2G7B0mr6jWCLwmCKCI8CezFEbvmolg+Byrl9rqaM/UqfF/jOMNk2yrXnhMnFjIO5HV0ojYlNEOcshs/oJKHFbYTo4M3zRnEUCx48yuCHKJaHUyYJIwW57D0HgsaSjxYJ9zKwbDBEYwO1/UZ+gZ7yOqAOyi1pTuZHpf8wDp9Vy1xGBsGV13E688OHW7Mdts8tCqxwzGuU9WthEsV3ENcwGkd2L6jqDa0jw7+OVj+9Mb7Ys5GRBCPaaKqD+rHB+qKjVdWyLpKUKoxXSFEOKQw9V57bcESYS/0JNnQYmAHshlSP1aqVRsRlfQm7kKU9S5SvJC3o5KUif8q5vTODSvlAlwAHufMsHJCUgVKqP88pLiXCHrBVRiwV+N4RXf+E=:ZSA0utWM5scQgXHEHWVzkciEmf3hzs8CrYRTJ8BVo8Q='}
                            )

    def process_sitemap(self, response):
        response.selector.remove_namespaces()
        store_urls = response.xpath('//urlset/url/loc/text()').extract()
        print(store_urls)

        for store_url in store_urls:
            print('processing', store_url)
            # Store urls look like: https://www.wawa.com/Stores/<STORE NUMBER>/...
            store_num = re.search(r'https://www\.wawa\.com/Stores/(\d+)/', store_url).group(1)

            wawa_params = {'storeNumber': store_num}
            yield scrapy.Request(
                'https://www.wawa.com/Handlers/LocationByStoreNumber.ashx?' + urlencode(wawa_params),
                headers={
                    'Accept': 'application/json',
                    'Host': 'www.wawa.com'
                },
                cookies = {'reese84': '3:dxpwQ5yByy9PqWsb714GBA==:hAwjbzbzIKd6d49Ipk9bvsE/pRsmNkAjX1OGKPkepY+omEu2G7B0mr6jWCLwmCKCI8CezFEbvmolg+Byrl9rqaM/UqfF/jOMNk2yrXnhMnFjIO5HV0ojYlNEOcshs/oJKHFbYTo4M3zRnEUCx48yuCHKJaHUyYJIwW57D0HgsaSjxYJ9zKwbDBEYwO1/UZ+gZ7yOqAOyi1pTuZHpf8wDp9Vy1xGBsGV13E688OHW7Mdts8tCqxwzGuU9WthEsV3ENcwGkd2L6jqDa0jw7+OVj+9Mb7Ys5GRBCPaaKqD+rHB+qKjVdWyLpKUKoxXSFEOKQw9V57bcESYS/0JNnQYmAHshlSP1aqVRsRlfQm7kKU9S5SvJC3o5KUif8q5vTODSvlAlwAHufMsHJCUgVKqP88pLiXCHrBVRiwV+N4RXf+E=:ZSA0utWM5scQgXHEHWVzkciEmf3hzs8CrYRTJ8BVo8Q='},
                callback=self.parse
            )

    def parse(self, response):
        print('parse')
        print(response.text)
        loc = json.loads(response.text)

        addr, city, state, zipc = self.get_addr(loc['addresses'][0])
        lat, lng = self.get_lat_lng(loc['addresses'][1])
        opening_hours = self.get_opening_hours(loc) or None

        properties = {
            'addr_full': addr,
            'name': loc['storeName'],
            'phone': loc['telephone'],
            'city': city,
            'state': state,
            'postcode': zipc,
            'ref': loc['locationID'],
            'website': "https://www.wawa.com/stores/{}/{}".format(
                loc['locationID'],
                loc['addressUrl'],
            ),
            'lat': float(lat),
            'lon': float(lng),
            'opening_hours': opening_hours,
            'extras': {
                'food': loc['amenities']['food'],
                'fuel': loc['amenities']['fuel'],
                'fuel_types': self.get_fuel_types(loc['fuelTypes']),
                'restrooms': loc['amenities']['restrooms'],
                'ethanol_free_gas': loc['amenities']['ethanolFreeGas'],
                'tesla_charging_station': loc['amenities']['teslaChargingStation'],
                'propane_exchange': loc['amenities']['propane'],
            }
        }

        print('parsed', properties)

        yield GeojsonPointItem(**properties)

    def get_addr(self, addr):
        return (v for k, v in addr.items() if k in ['address', 'city', 'state', 'zip'])

    def get_lat_lng(self, physical_addr):
        return physical_addr['loc']

    def get_opening_hours(self, store):
        open_time = store['storeOpen'][:5]
        close_time = store['storeClose'][:5]

        times = '{}-{}'.format(open_time, close_time)

        if times == '00:00-00:00':
            return '24/7'
        else:
            return times

    def get_fuel_types(self, fuel_types):
        types = []
        for fuel in fuel_types:
            types.append(fuel['description'].lower())

        return ';'.join(types)
