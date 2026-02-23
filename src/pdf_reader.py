from csv import excel
from urllib.request import urlopen
import os
import requests
import pandas as pd
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# DEFINES
excl_path = os.path.join(os.getcwd(), "Data", "GRI_2017_2020.xlsx")
data_path = os.path.join(os.getcwd(), "Data", "downloaded_pdfs.xlsx")
pdf_limiter = 30

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
    if pdf_limiter is None:
        pdf_limiter = len(df)
    urls = [url for url in df["Pdf_URL"] if url is not None]
    urls = urls[:pdf_limiter]  # Limit the number of URLs to download
    BRnums = [num for num in df["BRnum"] if num is not None]
    BRNums = BRnums[:pdf_limiter]
    return urls, BRNums

def exist(path): #Function to check if a file exists at the given path
    return os.path.exists(path)

def download_pdfs(urls, BRNums): #Function to download multiple PDFs and return their paths and statuses
    #Download multiple PDFs using thread pool for concurrent downloads
    data = []
    start = time.time()
    
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=10) as executor:  # 10 concurrent threads
        # Create a mapping of future to metadata
        future_to_metadata = {
            executor.submit(download_single_pdf, url, BRNum): (url, BRNum) 
            for url, BRNum in zip(urls, BRNums)
        }
        
        for i, future in enumerate(as_completed(future_to_metadata)):
            url, BRNum = future_to_metadata[future]
            
            try:
                path, status = future.result()
                if status:
                    data.append([BRNum, url, path, "Downloaded"])
                else:
                    data.append([BRNum, url, None, "Failed"])
            except Exception as e:
                print(f"Error processing {BRNum}: {e}")
                data.append([BRNum, url, None, "Failed"])
            
            # Status print
            print(f"Progress: {i+1}/{len(urls)} URLs processed. Elapsed: {time.time() - start:.2f}s")
    
    return data

def download_single_pdf(url, BRNum):
    #Helper function to download a single PDF (for threading)
    if pd.isna(url) or url.find(".pdf") == -1 or url.find("http") == -1:
        print(f"Skipping {url} - invalid PDF URL")
        return None, 0
    
    name = url.split("/")[-1].split(".pdf")[0]
    
    if exist(f"Pdf\\{name}.pdf"):
        print(f"{BRNum}: PDF already exists, skipping")
        return f"Pdf\\{name}.pdf", 1
    
    print(f"Downloading {BRNum} from {url}")
    return download_pdf(url, name, timeout=15)

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
        df = pd.DataFrame(total_data, columns=["BR_Num", "URL", "Path", "Status"])
    else:
        df = pd.DataFrame(data, columns=["BR_Num", "URL", "Path", "Status"])
    
    df.to_excel(path, index=False)

def clear_pdfs(set=False): #Function to clear the pdf folder for debugging purposes
    if set:
        for file in os.listdir("Pdf"):
            if file.endswith(".pdf"):
                os.remove(os.path.join("Pdf", file))

# MAIN
def main():
    global pdf_limiter
    pdf_limiter = 500
    #clear_pdfs(set=True)
    urls, BRNums = get_pdf_urls(excl_path)
    data = download_pdfs(urls, BRNums) 
    write_to_excel(data, data_path)


if __name__ == "__main__":
    main()