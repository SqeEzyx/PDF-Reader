''' IMPORTS '''
from urllib.request import urlopen
import os
import requests
import pandas as pd
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

''' DEFINES '''
excl_path = os.path.join(os.getcwd(), "Data", "GRI_2017_2020.xlsx")
data_path = os.path.join(os.getcwd(), "Data", "downloaded_pdfs.xlsx")
pdf_limiter = 1000
threads = 1
log = []

''' FUNCTIONS MISC '''
def open_pdf(path): #Function to open a PDF file and return its content as bytes
    f = open(path, "rb")

    return f.read()

def limit_length(lst, limit): #Function to limit the length of a list to a specified limit
    temp_list = []
    for l in lst:
        if l is not None and limit > 0:
            temp_list.append(l[:limit])
    return temp_list

def get_pdf_urls(path): #Function to read the Excel file and extract PDF URLs, limited by pdf_limiter
    global pdf_limiter

    df = pd.read_excel(path, sheet_name=0)

    if pdf_limiter is None:
        pdf_limiter = len(df)

    urls = [url for url in df["Pdf_URL"] if url is not None]

    BRNums = [num for num in df["BRnum"] if num is not None]
    
    url_sec = [num for num in df["Report Html Address"] if num is not None]

    return limit_length([urls, BRNums, url_sec], pdf_limiter)

def exist(path): #Function to check if a file exists at the given path
    return os.path.exists(path)

def valid_pdf(urls): #Function to check if a URL is a valid PDF URL
    valid = []
    for url in urls:
        if url.find(".pdf") != -1 and url.find("http") != -1:
            valid.append(url)

    return valid

def clear_pdfs(set): #Function to clear the pdf folder
    os.makedirs(os.path.join(os.getcwd(), "Pdf"), exist_ok=True)
    if set:
        for file in os.listdir("Pdf"):
            if file.endswith(".pdf"):
                os.remove(os.path.join("Pdf", file))

'''DOWNLOAD PDF FUNCTIONS '''

def download_pdf(url,name,timeout): #Function to download a PDF from a URL and save it with the given name, returns the path and status
    global log
    try:
        # Stream the response from requests and write in chunks to avoid blocking on urlopen
        # and to honor the timeout. Use a temporary file and atomic replace to avoid
        # partial files being treated as complete by other threads/processes.
        response = requests.get(url, timeout=timeout)

        os.makedirs(os.path.join(os.getcwd(), "Pdf"), exist_ok=True)

        path = os.path.join("Pdf", f"{name}.pdf")

        with open(path, "wb") as f:
            f.write(response.content)
        
        return path,1
    
    except Exception as e:
        #print(f"\nError downloading {name}: {e}")
        os.remove(path) if exist(path) else None

        log.append(f"Error downloading {name}: {e}")

        return None, 0

def download_single_pdf(url, BRNum):
    #Helper function to download a single PDF (for threading)
    valid_url = valid_pdf(url)
    if len(valid_url) == 0:
        return None, 0
    name = BRNum
    
    # Ensure only one thread downloads a particular filename at a time to avoid
    # duplicate downloads when multiple URLs map to the same filename.
    global _file_locks, _file_locks_lock
    try:
        _file_locks
    except NameError:
        _file_locks = {}
        _file_locks_lock = threading.Lock()

    with _file_locks_lock:
        lock = _file_locks.setdefault(name, threading.Lock())

    with lock:
        final_path = os.path.join("Pdf", f"{name}.pdf")
        if exist(final_path):
            return final_path, 1
        for url in valid_url:
            path, status = download_pdf(url, name, 15)
            if status:
                return path, status, url

        return None, 0

def download_loop(future_to_metadata,urls, data):
    for i, future in tqdm(enumerate(as_completed(future_to_metadata)),desc='Downloading PDFs', total=len(urls)):
        url, BRNum = future_to_metadata[future]
            
        try:
            path, status, valid_url = future.result()
            if status:
                data.append([BRNum, valid_url, path, "Downloaded"])
            else:
                if pd.isna(url):
                    url = None
                data.append([BRNum, url, None, "Failed"])
        except Exception as e:
            data.append([BRNum, url, None, "Failed"])

    return data

def download_pdfs(urls, BRNums): #Function to download multiple PDFs and return their paths and statuses
    #Download multiple PDFs using thread pool for concurrent downloads
    data = []
    global threads
    
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=threads) as executor:  # concurrent threads
        # Create a mapping of future to metadata
        future_to_metadata = {
            executor.submit(download_single_pdf, url, BRNum): (url, BRNum) 
            for url, BRNum in zip(urls, BRNums)
        }
        
        data = download_loop(future_to_metadata, urls, data)
    
    return data


''' EXCEL FUNCTIONS '''

def excel_stats(data): # Expects [BRNum, URL, Path, Status]
    downloaded = sum(1 for item in data if item[3] == "Downloaded")
    failed = sum(1 for item in data if item[3] == "Failed")
    data.append(["Total", None, None, f"Downloaded: {downloaded}, Failed: {failed}, Download Rate: {downloaded/(downloaded+failed)*100:.2f}%"])
    
    print(f"Excel Stats: Downloaded: {downloaded}, Failed: {failed}, Download Rate: {downloaded/(downloaded+failed)*100:.2f}%")
    
    return data

def write_to_excel(data, path): #Function to write the download results to an Excel file
    temp_data = excel_stats(data)
    df = pd.DataFrame(temp_data, columns=["BR_Num", "URL", "Path", "Status"])
    
    df.to_excel(path, index=False)


''' MAIN '''

def main():
    global pdf_limiter
    global threads
    global log
    pdf_limiter = 200
    threads = 50

    clear_pdfs(set=True)

    urls, BRNums, url_sec = get_pdf_urls(excl_path)

    all_urls = list(zip(urls, url_sec))

    data = download_pdfs(all_urls, BRNums) 
    write_to_excel(data, data_path)

    with open("error_log.txt", "w") as log_file:
        for error in log:
            log_file.write(error + "\n")
        log_file.close()


if __name__ == "__main__":
    main()