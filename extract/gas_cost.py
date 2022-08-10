from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import pandas as pd

#service = Service('./drivers/chromedriver')
#driver = webdriver.Chrome(service=service)
driver = webdriver.Chrome(executable_path='./drivers/chromedriver')
target_url ='https://emhu.mois.go.kr/mul/frt/biz/mul/publicItemList.do'
driver.get(target_url)
sleep(3)

#품목 : 도시가스 선택
select_product = Select(driver.find_element(By.ID, 'item'))
select_product.select_by_value('9')
sleep(1)


#column명이 될 row 추출 및 df생성
thead = driver.find_elements(By.CSS_SELECTOR, 'thead th')
thead_text = [('날짜') if i == 0 else thead[i].text for i in range(18)]
city_gas_price_df = pd.DataFrame(columns=thead_text)
id_num = 0


#연도별,월별 선택하여 조회 + 각각의 row를 df에 추가
for year_value in range(2012, 2022):
    year = str(year_value)
    select_year = Select(driver.find_element(By.ID, 'year'))
    select_year.select_by_value(year)
    sleep(1)
    for month_value in range(1, 13):
        month = str(month_value).zfill(2)
        select_month = Select(driver.find_element(By.ID, 'month'))
        select_month.select_by_value(month)
        sleep(1)
        input_button = driver.find_element(By.XPATH, '/html/body/form/div[1]/input')
        input_button.click()
        sleep(2)
        #row추출 후 df에 추가
        tbody = driver.find_element(By.CLASS_NAME, 'first').find_elements(By.TAG_NAME, 'td')
        tbody_text = [(year+month) if i == 0 else tbody[i].text for i in range(18)]
        city_gas_price_df.loc[id_num] = tbody_text
        id_num += 1

city_gas_price_df.to_csv("/home/big/predict_gas/raw_data/gas_cost/gas_cost.csv", index=True)
