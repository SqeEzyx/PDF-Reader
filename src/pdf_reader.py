''' IMPORTS '''
from urllib.request import urlopen
import os
import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

''' DEFINES '''
excl_path = os.path.join(os.getcwd(), "Data", "GRI_2017_2020.xlsx")
data_path = os.path.join(os.getcwd(), "Data", "downloaded_pdfs.xlsx")
pdf_limiter = 0
threads = 0

''' FUNCTIONS '''
def open_pdf(path): #Function to open a PDF file and return its content as bytes
    f = open(path, "rb")

    return f.read()

def get_pdf_urls(path): #Function to read the Excel file and extract PDF URLs, limited by pdf_limiter
    global pdf_limiter

    df = pd.read_excel(path, sheet_name=0)

    if pdf_limiter is None:
        pdf_limiter = len(df)

    urls = [url for url in df["Pdf_URL"] if url is not None]
    urls = urls[:pdf_limiter]  # Limit the number of URLs to download

    BRnums = [num for num in df["BRnum"] if num is not None]
    BRNums = BRnums[:pdf_limiter] # Limit the number of BRNums to download

    return urls, BRNums

def exist(path): #Function to check if a file exists at the given path
    return os.path.exists(path)


'''DOWNLOAD'''

def download_pdf(url,name,timeout): #Function to download a PDF from a URL and save it with the given name, returns the path and status
    try:
        # Stream the response from requests and write in chunks to avoid blocking on urlopen
        # and to honor the timeout. Use a temporary file and atomic replace to avoid
        # partial files being treated as complete by other threads/processes.
        response = requests.get(url, timeout=timeout, stream=True)

        os.makedirs(os.path.join(os.getcwd(), "Pdf"), exist_ok=True)

        final_path = os.path.join("Pdf", f"{name}.pdf")

        temp_path = final_path + ".part"

        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # atomic replace
        try:
            os.replace(temp_path, final_path)
        except Exception:
            # fallback to rename
            os.rename(temp_path, final_path)

        path = final_path
        
        if os.path.getsize(path) < 1000000:  # Check if the file is too small to be a valid PDF (heuristic) - 1 MB
            #print(path, " size: ", os.path.getsize(path), " - likely invalid PDF, deleting")
            os.remove(path)
            os.remove(temp_path)  # Ensure temp file is also removed if it exists
            return None, 0
        
        return path,1
    
    except Exception as e:
       #print(f"\nError downloading {name}: {e}")
        return None, 0

def download_single_pdf(url, BRNum):
    #Helper function to download a single PDF (for threading)
    if pd.isna(url) or url.find(".pdf") == -1 or url.find("http") == -1:
       #print(f"Skipping {url} - invalid PDF URL")
        return None, 0
    
    name = BRNum #url.split("/")[-1].split(".pdf")[0]
    
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
            #print(f"{BRNum}: PDF already exists, skipping")
            return final_path, 1

        #print(f"Downloading {BRNum} from {url}")
        return download_pdf(url, name, 15)

def download_loop(future_to_metadata,urls, data):
    for i, future in tqdm(enumerate(as_completed(future_to_metadata)),desc='Downloading PDFs', total=len(urls)):
        url, BRNum = future_to_metadata[future]
            
        try:
            path, status = future.result()
            if status:
                data.append([BRNum, url, path, "Downloaded"])
            else:
                if pd.isna(url):
                    url = None
                data.append([BRNum, url, None, "Failed"])
        except Exception as e:
           #print(f"Error processing {BRNum}: {e}")
            data.append([BRNum, url, None, "Failed"])
    
        # Status print
        #print(f"Progress: {i+1}/{len(urls)} URLs processed. Elapsed: {time.time() - start:.2f}s\n")
    return data

def download_pdfs(urls, BRNums): #Function to download multiple PDFs and return their paths and statuses
    #Download multiple PDFs using thread pool for concurrent downloads
    data = []
    global threads
    
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=threads) as executor:  # 20 concurrent threads
        # Create a mapping of future to metadata
        future_to_metadata = {
            executor.submit(download_single_pdf, url, BRNum): (url, BRNum) 
            for url, BRNum in zip(urls, BRNums)
        }
        executor.shutdown(wait=False,cancel_futures=False)  # Don't wait for all threads to finish here, we'll handle it in the loop below

        data = download_loop(future_to_metadata, urls, data)
        
    
    return data


''' EXCEL '''

def excel_stats(data):
    downloaded = sum(1 for item in data if item[3] == "Downloaded")
    failed = sum(1 for item in data if item[3] == "Failed")
    data.append(["Total", None, None, f"Downloaded: {downloaded}, Failed: {failed}, Download Rate: {downloaded/(downloaded+failed)*100:.2f}%"])
    
    print(f"Excel Stats: Downloaded: {downloaded}, Failed: {failed}, Download Rate: {downloaded/(downloaded+failed)*100:.2f}%")
    
    return data

def write_to_excel(data, path): #Function to write the download results to an Excel file
    temp_data = excel_stats(data)
    df = pd.DataFrame(temp_data, columns=["BR_Num", "URL", "Path", "Status"])
    
    df.to_excel(path, index=False)


def clear_pdfs(set=False): #Function to clear the pdf folder for debugging purposes
    if set:
        for file in os.listdir("Pdf"):
            if file.endswith(".pdf"):
                os.remove(os.path.join("Pdf", file))

''' MAIN '''

def main():
    global pdf_limiter
    global threads
    pdf_limiter = None
    threads = 100

    #clear_pdfs(set=True)

    urls, BRNums = get_pdf_urls(excl_path)
    data = download_pdfs(urls, BRNums) 
    write_to_excel(data, data_path)


if __name__ == "__main__":
    main()