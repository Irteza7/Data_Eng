from bs4 import BeautifulSoup
import requests

html="<!DOCTYPE html><html><head><title>Page Title</title></head><body><h3><b id='boldest'>Lebron James</b></h3><p> Salary: $ 92,000,000 </p><h3> Stephen Curry</h3><p> Salary: $85,000, 000 </p><h3> Kevin Durant </h3><p> Salary: $73,200, 000</p></body></html>"
soup = BeautifulSoup(html, "html.parser")
print(soup.prettify())
tag_object=soup.title
print("tag object:",tag_object)
print("tag object type:",type(tag_object))
tag_object=soup.h3
tag_object

tag_child =tag_object.b
tag_child

parent_tag=tag_child.parent
parent_tag

tag_object.parent

sibling_1=tag_object.next_sibling
sibling_1

sibling_2=sibling_1.next_sibling
sibling_2

salary=sibling_2.next_sibling

tag_child['id']
tag_child.attrs
tag_child.get('id')

tag_string=tag_child.string
tag_string
type(tag_string)
unicode_string = str(tag_string)
unicode_string


table="<table><tr><td id='flight'>Flight No</td><td>Launch site</td> <td>Payload mass</td></tr><tr> <td>1</td><td><a href='https://en.wikipedia.org/wiki/Florida'>Florida<a></td><td>300 kg</td></tr><tr><td>2</td><td><a href='https://en.wikipedia.org/wiki/Texas'>Texas</a></td><td>94 kg</td></tr><tr><td>3</td><td><a href='https://en.wikipedia.org/wiki/Florida'>Florida<a> </td><td>80 kg</td></tr></table>"
table_bs = BeautifulSoup(table, "html.parser")
table_tr = table_bs.find_all('tr')
table_tr[0]
tagtb = table_bs.find('tr')

import re
tag_regex = re.compile(r'<[^>]+>')
soup_tags = soup.find_all(string=tag_regex)
tags = tag_regex.findall(html)
soup.find(string='Lebron James')

###### Webscraping ######


import re
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import html5lib


data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])


url_scrape = 'https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks'
req_html = rq.get(url_scrape)
req_html.headers['Content-Type']
req_html.text[760:783]
soup = BeautifulSoup(req_html.text, "html.parser")
# soup.find(string = "By market capitalization").find_parent()
soup.find_all('tbody')[2].find_all('tr')
req_table = soup.find_all('table')[2].find_all('tr')
req_table[1].find_all('td')[1].text.strip()
req_table[1].find_all('a')[1].text.strip()

market_caps = []
banks = []
for row in req_table[1:]:
    market_caps.append(row.find_all('td')[2].text.strip())
    banks.append(row.find_all('td')[1].text.strip())
    data = pd.DataFrame({'Name': banks, 'Market Cap (US$ Billion)': market_caps})

data.head(5)
data.to_json('bank_marketcap.json')



#### Extract data using API ####
import requests as rq
import json

api_key = 'HN3j8UxsXIeZBhJ5XE95ZuHGxdSoq9jZ'
url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=HN3j8UxsXIeZBhJ5XE95ZuHGxdSoq9jZ"
response = rq.get(url)
response.status_code
response.headers['Content-Type']
response.text[:10]
res_json = json.loads(response.text)
# currency_data = pd.json_normalize(res_json['rates'])
currency_data = pd.DataFrame(res_json)
currency_data.head(5)
currency_data = currency_data['rates']
currency_pkr = currency_data.loc['PKR',]
currency_data.to_csv('exchange_rates_1.csv')