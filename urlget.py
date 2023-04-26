import requests
import os
from PIL import Image

url='https://www.ibm.com/'

r=requests.get(url)
r.status_code
print(r.request.headers)
print("request body:", r.request.body)
header=r.headers
print(r.headers)
header['Server']
header['Content-Type']
r.encoding
r.text[0:1000]

url1='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0101EN-SkillsNetwork/IDSNlogo.png'
r1 = requests.get(url1)
print(r1.headers)
path=os.path.join(os.getcwd(),'image.png')
path
with open(path,'wb') as f:
    f.write(r1.content)

Image.open(path)  

url2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0101EN-SkillsNetwork/labs/Module%205/data/Example1.txt'
r2 = requests.get(url2)
r.headers['Content-Type']
path1= os.path.join(os.getcwd(),'Exampletext1.txt')
with open(path1,'wb') as f1:
    f1.write(r2.content)


url_get='http://httpbin.org/get'
payload={"name":"Joseph","ID":"123"}
r=requests.get(url_get,params=payload)
r.url
print("request body:", r.request.body)
print(r.status_code)
print(r.text)
r.headers['Content-Type']
r.json()

url_post='http://httpbin.org/post'
r_post = requests.post(url_post,data=payload)
r_post.request.body
r_post.json()['form']


#####


from randomuser import RandomUser
import pandas as pd

r = RandomUser()
some_list = r.generate_users(10)
for user in some_list:
    print (user.get_full_name()," ",user.get_email())


# user.get_picture()

def users_info():
    users =[]
    for user in some_list:
        users.append({"Name":user.get_full_name(),"Gender":user.get_gender(),"City":user.get_city(),"State":user.get_state(),"Email":user.get_email(), "DOB":user.get_dob(),"Picture":user.get_picture()})
    return users

user_data = pd.DataFrame(users_info())

####

import json

data = requests.get("https://fruityvice.com/api/fruit/all")
# data.text[:100]
data.headers['Content-Type']
results = json.loads(data.text)
pd.DataFrame(results)
df2 = pd.json_normalize(results)
cherry = df2.loc[df2["name"] == 'Cherry']
(cherry.iloc[0]['family']) , (cherry.iloc[0]['genus'])
b_cal = df2.loc[df2['name']=='Banana']['nutritions.calories']

######

url3 = "https://api.openaq.org/v2/cities?limit=100&page=1&offset=0&sort=asc&order_by=city"
headers = {"accept": "application/json"}
r3 = requests.get(url3,headers=headers)
r3.headers['Content-Type']
results1 = json.loads(r3.text)
df3 = pd.json_normalize(results1['results'])

##just testing


