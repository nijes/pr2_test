from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from time import sleep
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import pandas as pd

#service = Service('./drivers/chromedriver.exe')
#driver = webdriver.Chrome(service=service)
driver = webdriver.Chrome(executable_path='./drivers/chromedriver')
target_url = 'https://data.kma.go.kr/climate/RankState/selectRankStatisticsDivisionList.do?pgmNo=179'
driver.get(target_url)
sleep(3)


# 표 컬럼명만 가져오기
soup = BeautifulSoup(driver.page_source, 'html.parser')
ths = soup.select('#div_01 > div.cont_itm > div.wrap_tbl > table.tbl > thead > tr > th')
lst = []
for i in range(len(ths)-1):
    lst.append(ths[i].text)
# print(lst)
# test = pd.DataFrame(lst)
sleep(2)


lst_A = [lst]
df = pd.DataFrame(columns=lst_A)
# print(df)
id = 0


# 지역/지점 선택 박스 클릭 (고정 작업)
for area in range(17):
    area_inputbox = driver.find_element(By.XPATH, '/html/body/div[1]/div[1]/div/div[2]/div[3]/div[3]/div[2]/form/div[1]/dl[2]/dd/input[1]')
    area_inputbox.click()
    sleep(2)

    # 지역 대분류 선택('+'클릭)
    area_1_select = driver.find_element(By.XPATH, f'/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[2+{area}]/a[1]')
    area_1_select.click()
    sleep(2)

    # 지역 중분류 선택: area 0~ 16
    if area == 0 : # 서울 -> 서울
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[2]/ul/li[2]/a[2]/label')
        area_2_select.click()
    elif area == 1 : # 부산 -> 부산
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[3]/ul/li/a[2]/label')
        area_2_select.click()
    elif area == 2 : # 대구 -> 대구
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[4]/ul/li[1]/a[2]/label')
        area_2_select.click()
    elif area == 3:  # 인천 -> 인천
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[5]/ul/li[3]/a[2]/label')
        area_2_select.click()
    elif area == 4:  # 광주 -> 광주
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[6]/ul/li/a[2]/label')
        area_2_select.click()
    elif area == 5:  # 대전 -> 대전
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[7]/ul/li/a[2]/label')
        area_2_select.click()
    elif area ==  6:  # 울산 -> 울산
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[8]/ul/li/a[2]/label')
        area_2_select.click()
    elif area == 7:  # 경기 -> 수원
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[9]/ul/li[2]/a[2]/label')
        area_2_select.click()
    elif area == 8:  # 강원 -> 강릉
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[10]/ul/li[1]/a[2]/label')
        area_2_select.click()
    elif area == 9:  # 충북 -> 충주
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[11]/ul/li[5]/a[2]/label')
        area_2_select.click()
    elif area == 10:  # 충남 -> 천안
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[12]/ul/li[5]/a[2]/label')
        area_2_select.click()
    elif area == 11:  # 전북 -> 전주
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[13]/ul/li[9]/a[2]/label')
        area_2_select.click()
    elif area == 12:  # 전남 -> 고흥
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[14]/ul/li[2]/a[2]/label')
        area_2_select.click()
    elif area == 13:  # 경북 -> 청송
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[15]/ul/li[13]/a[2]/label')
        area_2_select.click()
    elif area == 14:  # 경남 -> 김해
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[16]/ul/li[3]/a[2]/label')
        area_2_select.click()
    elif area == 15:  # 제주 -> 제주
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[17]/ul/li[6]/a[2]/label')
        area_2_select.click()
    else :  # 세종 -> 세종
        area_2_select = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/ul/li/ul/li[18]/ul/li/a[2]/label')
        area_2_select.click()

    sleep(2)

    # 선택완료 버튼 클릭
    select_button = driver.find_element(By.XPATH, '/html/body/div[4]/div/div/div/div[2]/div/a')
    select_button.click()
    sleep(2)

    # 시작 기간 선택
    start_year = Select(driver.find_element(By.ID, 'startYear'))
    start_year.select_by_value('1993')
    sleep(2)

    # 검색 버튼 클릭
    search_button = driver.find_element(By.XPATH,'/html/body/div[1]/div[1]/div/div[2]/div[3]/div[3]/div[2]/form/div[2]/button')
    search_button.click()
    sleep(2)

    # 로드된 페이지의 표 텍스트 크롤링
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    trs = soup.select('#rankStatTA > tr')

    for i in range(len(trs)):
        tds = trs[i].select('td')
        lst = []
        for j in range(len(tds) - 1):
            td_content = tds[j].text
            lst.append(td_content)
        #print(lst)
        df.loc[id]=lst
        id += 1

    sleep(2)

#print(df)
df.to_csv('/home/big/predict_gas/raw_data/temp/temp.csv', index=True)
