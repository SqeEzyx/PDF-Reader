import pdf_reader
import os
import pandas as pd

def download_pdf_test(url):
    name = "dummy"
    path, status = pdf_reader.download_pdf(url,name,15)

    assert path is not None
    assert status == 1
    
    os.remove(path)
    print("download_pdf Works")

def get_pdf_info_test(path):
    urls = pdf_reader.get_pdf_urls(path)
    assert urls is not None
    print("get_urls Works")

def write_to_excel_test(data, path):
    pdf_reader.write_to_excel(data, path)
    assert os.path.exists(path)
    os.remove(path)
    print("write_to_excel Works")

def excel_test(data,path):
    pdf_reader.write_to_excel(data, path)
    df = pd.read_excel(path)
    assert df is not None
    os.remove(path)
    print("Excel output Works")


def main():
    url = "https://assets.ctfassets.net/5ywmq66472jr/2SPRRF9gZuaMwXJFxKLKKj/573b714e0753db86e0faa86b2218bd5e/GRI_Index_2019.pdf"
    excl = os.path.join(os.getcwd(), "Data", "GRI_2017_2020.xlsx")
    
    download_pdf_test(url)
    get_pdf_info_test(excl)
    write_to_excel_test([("dummy", "dummy", "dummy", "Downloaded")], "Data\\test.xlsx")
    excel_test([["dummy", float('nan'), None, "Failed"]], "Data\\test.xlsx")


if __name__ == "__main__":
    main()