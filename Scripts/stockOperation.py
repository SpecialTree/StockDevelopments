from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.exceptions import Timeout
import time
import random
import os
from io import BytesIO
import datetime
import csv
import gc
import sys

class TimeExceed(Exception):
  pass

class StockOperation:
  #Here we initialize some headers for use in Yahoo finance and the SEC.
  #Genheaders will be used for Yahoo Finance, and SECheaders for the SEC.
  def __init__(self,OrgEmail,first3):
    #Please insert an email which you believe the SEC will recognize as a valid UA.
    #In addition, specify the first 3 digits of the current stable release of chrome.
    
    self.SECheaders = {"User-Agent":OrgEmail,'Accept': 'application/json, text/javascript, */*; q=0.01'}
    self.wd = os.path.join(os.path.expanduser('~'), 'FundOperation')
    self.Genheaders = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/" + first3 + ".0.0.0" + " Safari/537.36"}
  
  #This function prepares access to the SEC investment company class file.
  #This file contains every open fund (excluding money market funds) filed with the SEC!
  #The main data to scrounge from this file are the CIKs and SIDs associated with each fund.
  #In particular, the data identifying BlackRock, Vanguard, and SSGA funds are of interest.
  
  def linkFinder(self,year = 2022):
    link = requests.get('https://www.sec.gov/open/datasets-investment_company',headers = self.SECheaders,\
    timeout = (10,20))
    soup = BeautifulSoup(link.content,"html.parser")
    for a in soup.find_all('a'):
      if a.get('href') != None:
        if str(year) in a.get('href') and '.csv' in a.get('href'):
          return 'https://www.sec.gov/' + a.get('href')
  
  #This function actually acquires the data and puts it inside a new FundData folder.
  #Once you get the table of fundData, you can search for the relevant CIKs, and by extension SIDs
  #relevant to your search. I actually manually inspected the CIKs associated with the Big 3 funds,
  #so in the "FundSelector" function which follows it assumes you have constructed a file of the relevant
  #CIKs and SIDs.
  def investDownloader(self):
    url = self.linkFinder()
    
    if not os.path.exists(os.path.join(self.wd,'FundData')):
      os.mkdir(os.path.join(self.wd,'FundData'))
    if not os.path.exists(os.path.join(self.wd,'FundData','OGfile.csv')):
      downloadedCSV = requests.get(url,headers = self.SECheaders,timeout = (10,20),stream=True)
      with open(os.path.join(self.wd,'FundData','OGfile.csv'),'wb+') as f:
        for block in downloadedCSV.iter_content(1024 * 1024):
          f.write(block)
      del downloadedCSV,f,block
      gc.collect()


  def fundSelector(self,inp,out,yrs = 3,Vanguard = False):
    if os.path.exists(inp) == False:
      print('Please specify a valid path for the desired csv input.')
      return None
      
    url = 'https://efts.sec.gov/LATEST/search-index'
    Names = ['CIKs','SIDs','Fund_Tickers']

    if os.path.exists(out):
      cont = True
    else:
      cont = False
      better = open(out,'w')
      writer_object = csv.writer(better)
      writer_object.writerow(Names)
      better.close()
      del writer_object
      del better

    #cont portion
    if cont==True:
      old_stdout = sys.stdout
      sys.stdout = open(os.devnull, "w")

      with open(out, 'rb') as f:
        try:
          f.seek(-2, os.SEEK_END)
          while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        except OSError:
          f.seek(0)
        last_line = f.readline().decode()
      sys.stdout = old_stdout
      last_line = last_line.removesuffix('\r\r\n')
      print(last_line,flush = True)
    else:
      last_line = None
    
    with open(inp,'r') as myF:
      lineq = False
      for a in myF:
          currList = a.split(',')
          currCIK = currList[0].replace('"','').replace('"','')
          currSID = currList[1].replace('\n','').replace('"','').replace('"','')
          if [currCIK,currSID] == ['CIK.Number','Series.ID']:
              continue
          else:
              while len(currCIK) < 10:
                  currCIK = '0' + currCIK

              data = '{"q":"'+currSID+'","dateRange":"custom","category":"form-cat0","entityName":"' +\
              currCIK +'","enddt":"'+str(datetime.date.today())+'","startdt":"' + str((datetime.datetime.now() - datetime.timedelta(days=yrs*365)).date())+\
              '","filter_forms":"N-CEN"}'

              del currSID
              stopper = 0
              bigprob = 0


              while stopper == 0:
                try:
                  html = requests.post(url, headers=self.SECheaders, data=data,timeout = (10,20))
                  stopper +=1
                except requests.exceptions.ConnectionError:
                  if bigprob < 5:
                    print('Dealing with wireless issues...',flush = True)
                    time.sleep(10)
                    bigprob += 1
                  else:
                    raise(TimeExceed)
                except requests.exceptions.ReadTimeout:
                  if bigprob < 5:
                    print("Dealing with wireless issues...")
                    time.sleep(10)
                    bigprob += 1
                  else:
                    raise(TimeExceed)
              
              del data
              time.sleep(random.uniform(0.5,1))

              insoup = BeautifulSoup(html.content,"html.parser")
              intermed = insoup.get_text(",").split(",")
              m = []
              for b in intermed:

                if 'primary_doc.xml' in b:
                  m.append(b.replace('"_id":"','').replace('-','').replace(':primary_doc.xml"',''))

              if len(m) > 0:
                ncentv = sorted(m)[len(m) - 1]
              else:
                print('Skip',currCIK,flush=True)
                continue

              link = 'https://www.sec.gov/Archives/edgar/data/' + currCIK + '/' + ncentv + '/xslFormN-CEN_X01/primary_doc.xml'
              
              del ncentv
              del insoup
              del intermed
              del m
              gc.collect()

              stopper = 0
              bigprob = 0

              while stopper == 0:
                try:
                    fhtml = requests.get(link, headers=self.SECheaders,timeout = (10,20),stream=True)
                    stopper += 1
                except requests.exceptions.ConnectionError:
                    if bigprob < 5:
                        print("Dealing with wireless issues...")
                        time.sleep(10)
                        bigprob += 1
                    else:
                        raise(TimeExceed)
                except requests.exceptions.ReadTimeout:
                    if bigprob < 5:
                        print("Dealing with wireless issues...")
                        time.sleep(10)
                        bigprob += 1
                    else:
                        raise(TimeExceed)
              
              del link
              del stopper
              del bigprob
              time.sleep(random.uniform(0.5,1))

              secfile = open(self.wd + '\\tempFile.txt','wb+')
              for block in fhtml.iter_content(1024 * 1024):
                secfile.write(block)
              fhtml.close()
              secfile.close()
              del fhtml
              del secfile
              gc.collect()


              with open(self.wd + '\\tempFile.txt', 'r',encoding="utf8") as f:
                #Ticker and SID Selection
                tempList = []
                tempSID = None
                tempTicker = None
  
                c1 = False
                found1 = False
                c2 = False
                found2 = False
                isETF = False
  
                #Grab portion
  
                c3 = False
                b = False
                checkbox1 = False
                checkbox2 = False
                isindex = False
                checkboxF1 = False
                checkboxF2 = False
                deet = False
  
                #Main Block
                for line in f:
                  if not last_line or last_line == ''.join(Names).removesuffix('\r\n'):
                      if 'Item C.1. Background information.' in line:
                        c1 = True
        
                      if 'b. Series identication number, if any' in line and c1 == True:
                        found1 = True
        
                      if c1 == True and found1 == True and 'S' in line:
                        tempSID = line[line.find('class="fakeBox2">') + \
                        len('class="fakeBox2">'):line.find('class="fakeBox2">') + len('class="fakeBox2">')+10]
        
                      if 'd. Is this the first filing on this form by the' in line:
                        c1 = False
                        found1 = False


                      if Vanguard == False:
                          if 'Item C.2. Classes of open-end management investment companies.' in line:
                            c2 = True
        
                          if 'iii. Ticker symbol, if any' in line and c2 == True:
                            found2 = True
        
                          if c2 == True and found2 == True and 'class="fakeBox"' in line:
                            tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                            len('if any </td><td><p><div class="fakeBox">'):]
                            tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')
              
                          if 'Item C.3. Type of fund.' in line:
                            c2 = False
                            found2 = False
                      
                      else:
                          if 'Item C.2. Classes of open-end management investment companies.' in line:
                            c2 = True

                          if 'ETF Shares' in line and c2 == True:
                            isETF = True

                          if 'iii. Ticker symbol, if any' in line and c2 == True and isETF == True:
                            found2 = True

                          if c2 == True and found2 == True and isETF == True and 'class="fakeBox"' in line:
                            tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                            len('if any </td><td><p><div class="fakeBox">'):]
                            tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')
                            tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr><tr><td>\n')
                            isETF = False

                          if 'Item C.3. Type of fund.' in line:
                            c2 = False
                            found2 = False
                            isETF = False
        
        
        
                      if 'Item C.3. Type of fund.' in line:
                        c3 = True

                      if '/Images/box-unchecked.jpg' in line  and c3 == True:
                        checkboxF1 = True
                      
                      if checkboxF1 == True and c3 == True and 'ii.' in line:
                          checkboxF2 = True
                      
                      if checkboxF1 == True and c3 == True and checkboxF2 == True and '/Images/box-unchecked.jpg' in line:
                          checkboxF1 = False
                          c3 = False
                          checkboxF2 = False
                      
                      if 'Exchange-Traded Managed Fund' in line:
                          deet = False
                          checkboxF1 = False
                          checkboxF2 = False
                          
        
                      if 'b.' in line and c3 == True:
                        b = True
        
                      if '/Images/box-checked.jpg' in line and c3 == True and b == True:
                        checkbox1 = True
              
                      if 'Index Fund' in line and checkbox1 == True and c3 == True and b == True:
                        isindex = True
        
                      if '/Images/box-checked.jpg' in line and checkbox1 == True and isindex == True and c3 == True and b == True:
                        checkbox2 = True
        
                      if 'Seeks to achieve performance results that are a multiple' in line and checkbox1 == True and \
                        isindex == True and checkbox2 == True and c3 == True and b == True:
                        checkbox1 = False
                        checkbox2 = False
                        isindex = False
                        c3 = False
                        b = False
        
                      if 'Item C.4. Diversification.' in line and checkbox1 == True and isindex == True and checkbox2 == False \
                        and c3 == True and b == True:
                        tempList.append(currCIK)
                        tempList.append(tempSID)
                        tempList.append(tempTicker)
                        with open(out,'a') as handle:
                          writer_object = csv.writer(handle)
                          writer_object.writerow(tempList)
                        tempList = []
                        tempSID = None
                        tempTicker = None
              
                        checkbox1 = False
                        isindex = False
                        c3 = False
                        b = False
                        gc.collect()
                  else:
                      if lineq:
                          if 'Item C.1. Background information.' in line:
                            c1 = True

                          if 'b. Series identication number, if any' in line and c1 == True:
                            found1 = True

                          if c1 == True and found1 == True and 'S' in line:
                            tempSID = line[line.find('class="fakeBox2">') + \
                            len('class="fakeBox2">'):line.find('class="fakeBox2">') + len('class="fakeBox2">')+10]

                          if 'd. Is this the first filing on this form by the' in line:
                            c1 = False
                            found1 = False

 
                          if Vanguard == False:
                              if 'Item C.2. Classes of open-end management investment companies.' in line:
                                c2 = True

                              if 'iii. Ticker symbol, if any' in line and c2 == True:
                                found2 = True

                              if c2 == True and found2 == True and 'class="fakeBox"' in line:
                                tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                                len('if any </td><td><p><div class="fakeBox">'):]
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')

                              if 'Item C.3. Type of fund.' in line:
                                c2 = False
                                found2 = False

                          else:
                              if 'Item C.2. Classes of open-end management investment companies.' in line:
                                c2 = True

                              if 'ETF Shares' in line and c2 == True:
                                isETF = True

                              if 'iii. Ticker symbol, if any' in line and c2 == True and isETF == True:
                                found2 = True

                              if c2 == True and found2 == True and isETF == True and 'class="fakeBox"' in line:
                                tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                                len('if any </td><td><p><div class="fakeBox">'):]
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr><tr><td>\n')
                                isETF = False

                              if 'Item C.3. Type of fund.' in line:
                                c2 = False
                                found2 = False
                                isETF = False       
                         

                          if 'Item C.3. Type of fund.' in line:
                            c3 = True

                          if 'Exchange-Traded Fund or Exchange-Traded Managed Fund or offers a Class that itself' in line and c3 ==True:
                            deet = True

                          if '/Images/box-unchecked.jpg' in line and deet == True and c3 == True:
                            checkboxF1 = True
                            print(line.count('/Images/box-unchecked.jpg'),flush = True)

                          if '/Images/box-unchecked.jpg' in line and deet == True and checkboxF1 == True and c3 == True:
                            checkboxF1 = False
                            deet = False
                            c3 = False

                          if 'b.' in line and c3 == True:
                            b = True

                          if '/Images/box-checked.jpg' in line and c3 == True and b == True:
                            checkbox1 = True

                          if 'Index Fund' in line and checkbox1 == True and c3 == True and b == True:
                            isindex = True

                          if '/Images/box-checked.jpg' in line and checkbox1 == True and isindex == True and c3 == True and b == True:
                            checkbox2 = True

                          if 'Seeks to achieve performance results that are a multiple' in line and checkbox1 == True and \
                            isindex == True and checkbox2 == True and c3 == True and b == True:
                            checkbox1 = False
                            checkbox2 = False
                            isindex = False
                            c3 = False
                            b = False

                          if 'Item C.4. Diversification.' in line and checkbox1 == True and isindex == True and checkbox2 == False \
                            and c3 == True and b == True:
                            tempList.append(currCIK)
                            tempList.append(tempSID)
                            tempList.append(tempTicker)
                            with open(out,'a') as handle:
                              writer_object = csv.writer(handle)
                              writer_object.writerow(tempList)
                              tempList = []
                            tempList = []
                            tempSID = None
                            tempTicker = None

                            checkbox1 = False
                            isindex = False
                            c3 = False
                            b = False
                            gc.collect()
                      else:
                          if 'Item C.1. Background information.' in line:
                            c1 = True

                          if 'b. Series identication number, if any' in line and c1 == True:
                            found1 = True

                          if c1 == True and found1 == True and 'S' in line:
                            tempSID = line[line.find('class="fakeBox2">') + \
                            len('class="fakeBox2">'):line.find('class="fakeBox2">') + len('class="fakeBox2">')+10]

                          if 'd. Is this the first filing on this form by the' in line:
                            c1 = False
                            found1 = False


                          if Vanguard == False:
                              if 'Item C.2. Classes of open-end management investment companies.' in line:
                                c2 = True

                              if 'iii. Ticker symbol, if any' in line and c2 == True:
                                found2 = True

                              if c2 == True and found2 == True and 'class="fakeBox"' in line:
                                tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                                len('if any </td><td><p><div class="fakeBox">'):]
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')

                              if 'Item C.3. Type of fund.' in line:
                                c2 = False
                                found2 = False

                          else:
                              if 'Item C.2. Classes of open-end management investment companies.' in line:
                                c2 = True

                              if 'ETF Shares' in line and c2 == True:
                                isETF = True

                              if 'iii. Ticker symbol, if any' in line and c2 == True and isETF == True:
                                found2 = True

                              if c2 == True and found2 == True and isETF == True and 'class="fakeBox"' in line:
                                tempTicker = line[line.find('if any </td><td><p><div class="fakeBox">') + \
                                len('if any </td><td><p><div class="fakeBox">'):]
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr></tr></table><br><h4>\n')
                                tempTicker = tempTicker.removesuffix('<span>\xa0</span></div></p></td></tr><tr><td>\n')
                                isETF = False

                              if 'Item C.3. Type of fund.' in line:
                                c2 = False
                                found2 = False
                                isETF = False 
                          

                          if 'Item C.3. Type of fund.' in line:
                            c3 = True
                          
                          if 'Exchange-Traded Fund or Exchange-Traded Managed Fund or offers a Class that itself' in line and c3 ==True:
                            deet = True

                          if '/Images/box-unchecked.jpg' in line and deet == True and c3 == True:
                            checkboxF1 = True

                          if '/Images/box-unchecked.jpg' in line and deet == True and checkboxF1 == True and c3 == True:
                            checkboxF1 = False
                            deet = False
                            c3 = False

                          if 'b.' in line and c3 == True:
                            b = True

                          if '/Images/box-checked.jpg' in line and c3 == True and b == True:
                            checkbox1 = True

                          if 'Index Fund' in line and checkbox1 == True and c3 == True and b == True:
                            isindex = True

                          if '/Images/box-checked.jpg' in line and checkbox1 == True and isindex == True and c3 == True and b == True:
                            checkbox2 = True

                          if 'Seeks to achieve performance results that are a multiple' in line and checkbox1 == True and \
                            isindex == True and checkbox2 == True and c3 == True and b == True:
                            checkbox1 = False
                            checkbox2 = False
                            isindex = False
                            c3 = False
                            b = False

                          if 'Item C.4. Diversification.' in line and checkbox1 == True and isindex == True and checkbox2 == False \
                            and c3 == True and b == True:
                            tempList.append(currCIK)
                            tempList.append(tempSID)
                            tempList.append(tempTicker)

                            checkbox1 = False
                            isindex = False
                            c3 = False
                            b = False
                            gc.collect()

                          if last_line == ','.join(tempList):
                            lineq = True
                            tempList = []
                          elif not lineq:
                            tempList = []

              f.close()
              del f,tempList,tempSID,tempTicker,c1,found1,c2,found2,c3,b,checkbox1,checkbox2,isindex,isETF
              gc.collect()
              os.remove(self.wd + '\\tempFile.txt')


  def holdingsFinder(self,inp,out,fundDir,pos = 0,yrs = 5):
    tempdf = pd.read_csv(inp)
    tempdf = tempdf.astype(str)
    tempdfNames = [a for a in tempdf.columns]
    tempdfNames.append('ISINs and Share Counts')
    
    url = 'https://efts.sec.gov/LATEST/search-index'

    o = len(tempdf['Tickers'])
    xISIN = '<span>ISIN</span></td><td><div class="fakeBox3">'
    xVol = '</td><td><div class="fakeBox4">'


    for a in range(pos,o):
      z = tempdf.iloc[a,:].to_list()
      while len(z[0]) < 10:
          z[0] = '0' + z[0]

      m = []
      repdate = []
      filedate = []
      stopper = 0
      bigprob = 0
      data = '{"q":"' + z[1] +'","dateRange":"custom","category":"form-cat0","entityName":"' +\
      z[0] +'","enddt":"'+str(datetime.date.today())+'","startdt":"'+str((datetime.datetime.now() - datetime.timedelta(days=yrs*365)).date())+'","filter_forms":"NPORT-P"}'
      while stopper == 0:
        try:
            html = requests.post(url, headers=self.SECheaders, data=data,timeout = (10,20))
            stopper +=1
        except requests.exceptions.ConnectionError:
            if bigprob < 5:
                print("Dealing with wireless issues...",flush = True)
                time.sleep(10)
                bigprob += 1
            else:
                raise(TimeExceed)
        except requests.exceptions.ReadTimeout:
            if bigprob < 5:
                print("Dealing with wireless issues...",flush = True)
                time.sleep(10)
                bigprob += 1
            else:
                raise(TimeExceed)
      
      del data
      del stopper
      del bigprob
      time.sleep(random.uniform(0.5,1))
      insoup = BeautifulSoup(html.content,"html.parser")
      intermed = insoup.get_text(",").split(",")
      for b in intermed:
                  
        if 'primary_doc.xml' in b:
            m.append(b.replace('"_id":"','').replace('-','').replace(':primary_doc.xml"',''))

      if len(m) > 0:
        m.sort() 
      else:
          print('Skip',flush=True)
          print(a+1,'of',o,'done.',flush = True)
          continue

      for b in intermed:
        if "period_ending" in b:
          tempstr = b.removeprefix('"period_ending":').replace('"','').replace('"','')
          if 'range' not in tempstr:
            repdate.append(datetime.datetime.strptime(tempstr,"%Y-%m-%d"))
        if "file_date" in b:
          tempstr = b.removeprefix('"file_date":').replace('"','').replace('"','')
          if 'range' not in tempstr:
            filedate.append(datetime.datetime.strptime(tempstr,"%Y-%m-%d"))


      repdate.sort()
      filedate.sort()
      comp = True

      while comp:
        comp = False
        for b in range(0,len(repdate)):
          if repdate.count(repdate[b])>1:
            repdate.remove(repdate[b])
            filedate.remove(filedate[b])
            m.remove(m[b])
            comp = True
            break

      for c in range(0,len(m)):
          nportv = m[c] 
          link = 'https://www.sec.gov/Archives/edgar/data/' + z[0] + '/' + nportv + '/xslFormNPORT-P_X01/primary_doc.xml'
          stopper = 0
          bigprob = 0

          while stopper == 0:
            try:
                fhtml = requests.get(link, headers=self.SECheaders,timeout = (10,20),stream=True)
                stopper += 1
            except requests.exceptions.ConnectionError:
                if bigprob < 5:
                    print("Dealing with wireless issues...",flush = True)
                    time.sleep(10)
                    bigprob += 1
                else:
                    raise(TimeExceed)
            except requests.exceptions.ReadTimeout:
                if bigprob < 5:
                    print("Dealing with wireless issues...",flush = True)
                    time.sleep(10)
                    bigprob += 1
                else:
                    raise(TimeExceed)
          del link
          del stopper
          del bigprob
          time.sleep(random.uniform(0.5,1))

          secfile = open(os.path.join(self.wd + 'tempFile.txt'),'wb+')
          for block in fhtml.iter_content(1024 * 1024):
            secfile.write(block)
          fhtml.close()
          secfile.close()
          del fhtml,secfile,block
          gc.collect()
          
          chunkT = False
          currISIN = None
          balNext = False
          currWant = None
          fundloc = ''
          writer_object = None
          handle = None
          
          with open(os.path.join(self.wd,'tempFile.txt'),'r') as f:
            for line in f:
                line = (len(line)>0 and line or "0")
                if 'Item C.1. Identification of investment.' in line:
                  chunkT = True

                if chunkT and xISIN in line:
                  currISIN = line[line.find(xISIN)+len(xISIN):line.find(xISIN)+len(xISIN)+12]
                  
                if chunkT and currISIN and balNext:
                  currWant = line[line.find(xVol) + len(xVol):]
                  currWant = currWant[:currWant.find('<span>')].strip()
                  balNext = False

                elif chunkT and currISIN and 'For derivatives contracts, as applicable, provide the number of' in line:
                  balNext = True

                if 'Number of shares' in line and chunkT and currISIN and currWant:
                  fundloc = os.path.join(self.wd,fundDir,z[2]+'_'+str(repdate[c])[0:10]+'.csv')
                  if os.path.exists(fundloc):
                    with open(fundloc,'a') as handle:
                      writer_object = csv.writer(handle)
                      writer_object.writerow([currISIN,currWant])
                  else:
                    with open(fundloc,'w') as handle:
                      writer_object = csv.writer(handle)
                      writer_object.writerow(['ISINs','Volumes'])
                      writer_object.writerow([currISIN,currWant])

                if 'Item C.3. Indicate payoff profile among the following categories' in line:
                  chunkT = False
                  currISIN = None
                  balNext = False
                  currWant = None
          
          if writer_object:
              del writer_object
          if handle:
              del handle
          f.close()
          del f,chunkT,currISIN,balNext,currWant,fundloc
          gc.collect()
          os.remove(os.path.join(self.wd,'tempFile.txt'))
          print('success',flush=True)
          print(a+1,'of',o,'done',str(repdate[c])[0:10],flush=True)
      del z

  def UniqueObsSelect(self,inp,out):
    #ICYFlame Solution
    with open(inp, 'r') as in_file, open(out, 'w') as out_file:
      seen = set()
      for line in in_file:
        if line.replace('\r\n','') in seen: continue

        seen.add(line.replace('\r\n',''))
        out_file.write(line.replace('\r\n',''))
    os.remove(inp)
  
  
  def UniqueISINs(self,mydirs,out):
    myset = set()
    for a in mydirs:
      fileList = os.listdir(a)
      for b in fileList:
        with open(a+'/'+b,'r') as f:
          for c in f:
            if not "ISINs" in c:
              myset.add(c.split(',')[0].strip().replace('\n','').replace('\n','').replace('\t',''))
            else:
              continue
    
    with open(out,'w') as final:
      writer_object = csv.writer(final)
      for d in myset:
        writer_object.writerow(d.replace('\n','').replace('\r','').replace('\t',''))

  
  def RemoveBlankObs(self,inp,out,enc = 'utf8'):
    old_stdout = sys.stdout
    sys.stdout = open(os.devnull, "w")
    with open(inp,'r',encoding = enc) as in_file:
        with open(out, 'w', newline='') as out_file:
            writer = csv.writer(out_file)
            for row in csv.reader(in_file):
                if row:
                    writer.writerow(row)
    sys.stdout = old_stdout
    os.remove(inp)
  
  
  def MarketCap(self,ticker):
    url ='https://finance.yahoo.com/quote/' + ticker
    
    stopper = 0
    bigprob = 0
    while stopper == 0:
      try:
          fhtml = requests.get(url, headers=self.Genheaders,timeout = (10,20))
          stopper += 1
      except requests.exceptions.ConnectionError:
          if bigprob < 5:
              print("Dealing with wireless issues...",flush = True)
              time.sleep(10)
              bigprob += 1
          else:
              raise(TimeExceed)
      except requests.exceptions.ReadTimeout:
          if bigprob < 5:
              print("Dealing with wireless issues...",flush = True)
              time.sleep(10)
              bigprob += 1
          else:
              raise(TimeExceed)
    del stopper
    del bigprob
    time.sleep(random.randint(3,5))
    
    with open(os.path.join(self.wd,'tempFile2.txt'),'wb+') as f:
      for block in fhtml.iter_content(1024 * 1024):
        f.write(block)
      f.seek(0)
      found = False
      for line in f:
        deline = line.decode()
        if 'MARKET_CAP-value' in deline:
          deline = deline[deline.find('MARKET_CAP-value">') + len('MARKET_CAP-value">'):]
          deline = deline[:deline.find('</td')]
          break
        else:
          deline = 'None'
      os.remove(os.path.join(self.wd,'tempFile2.txt'))
      return deline
  
  
  def TickerInfo(self,inp,out,pos=0):
    if os.path.exists(out) and pos != 0:
      count = pos
      pass
    else:
      final = open(out,'w')
      writer_object = csv.writer(final)
      writer_object.writerow(['Ticker','Sector','Industry','MarketCap','ISIN'])
      final.close()
      count = 0
    
    with open(inp,'r',encoding = "iso-8859-1") as file:
        lines = sum(1 for line in file if line.strip() != '')
    with open(inp,'r',encoding = "iso-8859-1") as f:
      for index,mystr in enumerate(f):
        found = True
        if index < pos:
          found = False
        if found:
            a = mystr.strip()
            if a != '':
                a = ''.join(a.split(','))
                
                url = 'https://query1.finance.yahoo.com/v1/finance/search?q='+a+'&lang=en-'
                
                stopper = 0
                bigprob = 0
                ticker = None
                sector = None
                industry = None
  
                while stopper == 0:
                  try:
                      fhtml = requests.get(url, headers=self.Genheaders,timeout = (10,20))
                      stopper += 1
                  except requests.exceptions.ConnectionError:
                      if bigprob < 5:
                          print("Dealing with wireless issues...",flush = True)
                          time.sleep(10)
                          bigprob += 1
                      else:
                          raise(TimeExceed)
                  except requests.exceptions.ReadTimeout:
                      if bigprob < 5:
                          print("Dealing with wireless issues...",flush = True)
                          time.sleep(10)
                          bigprob += 1
                      else:
                          raise(TimeExceed)
                del stopper
                del bigprob
                
                time.sleep(random.randint(3,5))
                content = fhtml.content.decode()
                
                if '"symbol"' in content:
                  ticker = content[content.find('"symbol"') + len('"symbol"')+2:content.\
                  find(',',content.find('"symbol"')) - 1]
                  MakCap = self.MarketCap(ticker)
                  with open(out,'a') as final:
                    writer_object = csv.writer(final)
                    if '"sector"' in content:
                      sector = content[content.find('"sector"') + len('"sector"')+2:content.\
                      find(',',content.find('"sector"')) - 1]
                    
                    if '"industry"' in content:
                      industry = content[content.find('"industry"') + len('"industry"')+2:content.\
                      find(',',content.find('"industry"')) - 1]

                    if ticker and industry and sector:
                      writer_object.writerow([ticker,sector,industry,Makcap,a])
                    elif ticker and sector:
                      writer_object.writerow([ticker,sector,'NA',Makcap,a])
                    else:
                      writer_object = csv.writer(final)
                      writer_object.writerow([ticker,'NA','NA',Makcap,a])
                else:
                  del content,fhtml
                  gc.collect()
                  count +=1
                  print('Skipped',flush = True)
                  print(count,'of',lines,'done.',flush = True)
                  continue
                del content,fhtml
                gc.collect()
                count +=1
                print('Success.',flush = True)
                print(count,'of',lines,'done.')
      

            
 
    
    
    

