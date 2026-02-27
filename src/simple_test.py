import pandas as pd
import os
import requests

url = "http://www.ablerex.com.tw/ch/CSR/CSR20170911.pdf"

path = "test.pdf"

response = requests.get(url, timeout=15, stream=True)


# After attempting download (or if file already exists), try to extract an HTML message
if os.path.exists(path):
    try:
        with open(path, "rb") as fb:
            data = fb.read()

        # decode bytes to text (ignore decoding errors)
        text = data.decode("utf-8", errors="ignore")

        import re

        # look for the HTML comment marker and capture the following text up to the next '<'
        m = re.search(r'<!--Message-->([^<]*)', text, re.S)
        if m:
            message = m.group(1).strip()
            print("Found message:", message)
        else:
            print("No <!--Message--> marker found in", path)
    except Exception as e:
        print("Error reading or parsing", path, ":", e)
