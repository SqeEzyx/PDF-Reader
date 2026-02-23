import pdf_reader
import os
import pandas as pd

def open_pdf_test(fp):
    f = pdf_reader.open_pdf(fp)
    assert f is not None
    
    print("open_pdf Works")

def download_pdf_test(url):
    name = "dummy"
    path, status = pdf_reader.download_pdf(url,name)

    assert path is not None
    assert status == 1

    open_pdf_test(path)
    
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
    url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
    excl = "C:\\Users\\SPAC-O-4\\Documents\\GitHub\\PDF-Reader\\Data\\GRI_2017_2020.xlsx"
    
    download_pdf_test(url)
    get_pdf_info_test(excl)
    write_to_excel_test([("dummy", "dummy", 1)], "Data\\test.xlsx")
    excel_test([["dummy", float('nan'), "Failed"]], "Data\\test.xlsx")
    excel_test([["dummy", "dummy", "Downloaded"]], "Data\\test.xlsx")


if __name__ == "__main__":
    main()