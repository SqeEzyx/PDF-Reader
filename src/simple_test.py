import pandas as pd
import os
import requests

url = "http://www.ablerex.com.tw/ch/CSR/CSR20170911.pdf"

path = "test.pdf"

response = requests.get(url, timeout=15, stream=True)

with open(path, "wb") as f:
    f.write(response.content)