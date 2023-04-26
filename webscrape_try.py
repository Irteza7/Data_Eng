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