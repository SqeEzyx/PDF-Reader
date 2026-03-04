from cmath import nan

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

Brnums = ['BR50079', 'BR50080', 'BR50081', 'BR50082', 'br2']
#URL Sections: [nan, nan, 'http://www.accentuateltd.co.za/pdf/financials/50_Accentuate_Annual_Report_2017.pdf', 'https://www.accenture.com/us-en/company-corporate-citizenship?c=glb_corcitify17acc_10000018&n=otc_0317', 'http://www.ablerex.com.tw/ch/CSR/CSR20170911.pdf']
#all_urls = [('http://acceleratepf.co.za/pdf/Accelerate_IR_11035_20170619_V04d_LN.pdf', nan), ('http://pdf.dfcfw.com/pdf/H2_AN201703080393596400_01.pdf', nan), (nan, 'http://www.accentuateltd.co.za/pdf/financials/50_Accentuate_Annual_Report_2017.pdf'), ('https://www.accenture.com/t20170329T044918__w__/us-en/_acnmedia/PDF-48/Accenture-2016-Corporate-Citizenship-Report.pdf', 'https://www.accenture.com/us-en/company-corporate-citizenship?c=glb_corcitify17acc_10000018&n=otc_0317'), ('https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf', 'http://www.ablerex.com.tw/ch/CSR/CSR20170911.pdf')]

urls,brnums,sec_url =pdf_reader.get_pdf_urls("Data//dummy.xlsx")

all_urls = list(zip(urls, sec_url))

#print(pdf_reader.download_single_pdf(urls[0], "dummy"))
data = pdf_reader.download_pdfs(all_urls, Brnums)
pdf_reader.write_to_excel(data, "Data//dummy_stats.xlsx")