from csv import excel
from urllib.request import urlopen
import os
import requests
import pandas as pd


# DEFINES
excl_path = os.path.join(os.getcwd(), "Data", "GRI_2017_2020.xlsx")
data_path = os.path.join(os.getcwd(), "Data", "downloaded_pdfs.xlsx")
pdf_limiter = 10

# FUNCTIONS
def open_pdf(path): #Function to open a PDF file and return its content as bytes
    f = open(path, "rb")

    return f.read()

def download_pdf(url,name,timeout=30): #Function to download a PDF from a URL and save it with the given name, returns the path and status
    try:
        response = requests.get(url, timeout=timeout)

        with open(f"Pdf\\{name}.pdf", "wb") as f:
            f.write(urlopen(response.url).read())
        path = f"Pdf\\{name}.pdf"
    except Exception as e:
        print(f"\nError downloading {name}: {e}")
        path = None
        return path, 0
    
    return path,1

def get_pdf_urls(path): #Function to read the Excel file and extract PDF URLs, limited by pdf_limiter
    global pdf_limiter
    df = pd.read_excel(path, sheet_name=0)

    urls = [url for url in df["Pdf_URL"] if url is not None]
    urls = urls[:pdf_limiter]  # Limit the number of URLs to download
    return urls

def exist(path): #Function to check if a file exists at the given path
    return os.path.exists(path)

def download_pdfs(urls): #Function to download multiple PDFs and return their paths and statuses
    data = []
    temp_data = []
    for url in urls:
        if pd.isna(url) or url.find(".pdf") == -1:
            print(f"\nSkipping {url} as it does not contain a PDF link.")
            temp_data.append([None, None, 0])
            continue

        name = url.split("/")[-1].split(".pdf")[0]

        if exist(f"Pdf\\{name}.pdf"): #Check if the PDF already exists to avoid redundant downloads
            print(f"already exists, skipping download.")
            temp_data.append([url, f"Pdf\\{name}.pdf", 1])
            continue
        
        #Status Print
        print(f"\nDownloading {name} from {url}...")
        print(f"Current progress: {len(temp_data)}/{len(urls)} URLs processed.")

        path,status = download_pdf(url,name,timeout=5)
        if status:
            print(f"\nDownloaded")
        else:
            print(f"\nFailed to download")

        temp_data.append([url,path,status])

    data = temp_data.copy()
    for i, item in enumerate(temp_data):
        if item[2] == 1:
            data[i] = [item[0], item[1], "Downloaded"]
        else:
            data[i] = [item[0], None, "Failed"]

    return data

def format_data(data,path): #Function to format the data for Excel output, replacing status codes with descriptive text
    total_data = pd.read_excel(path).values.tolist()
    temp_data = total_data.copy()
    
    for items in temp_data:
        for i, item in enumerate(items):
            if pd.isna(item):
                items[i] = None
                
    for item in data:
        if item not in temp_data:
            temp_data.append(item)

    return temp_data

def write_to_excel(data, path): #Function to write the download results to an Excel file
    if exist(path):
        total_data = format_data(data,path)
        df = pd.DataFrame(total_data, columns=["URL", "Path", "Status"])
    else:
        df = pd.DataFrame(data, columns=["URL", "Path", "Status"])
    
    df.to_excel(path, index=False)

def clear_pdfs(): #Function to clear the pdf folder for debugging purposes
    for file in os.listdir("Pdf"):
        if file.endswith(".pdf"):
            os.remove(os.path.join("Pdf", file))

# MAIN
def main():
    urls = get_pdf_urls(excl_path)
    data = download_pdfs(urls)
    write_to_excel(data, data_path)


if __name__ == "__main__":
    main()