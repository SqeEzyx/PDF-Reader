import pandas as pd
import os
import requests
import pdf_reader

pdf_reader.clear_pdfs(True)


urls = [("http://www.ablerex.com.tw/ch/CSR/CSR20170911.pdf", "nan"),
        ("http://www.scania.com/content/dam/group/investor-relations/financial-reports/annual-reports/Scania_AnnualReport_2019-English.pdf", "nan"),
        ("", "nan"),
        ("http://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf", "nan")]
brnums = ["1","2","3","4"]
#print(pdf_reader.download_single_pdf(urls[0], "dummy"))
pdf_reader.excel_stats(pdf_reader.download_pdfs(urls, brnums))