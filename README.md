# 도시가스 수요량 및 공급량 예측을 위한 데이터셋 대시보드
<br>

>도시가스 수요 및 공급 예측을 위한 데이터셋 대시보드를 구축하였습니다.<br>
완성한 대시보드 웹 페이지는 [여기](https://phoenix-predictgas.herokuapp.com/)서 확인 가능합니다. 


프로젝트 기간: 22.07.28 ~ 2022.08.17
<br><br>


## 프로젝트 배경
* '가스공사의 수요예측 실패'와 같은 기사를 접하면서 도시가스의 수요량 및 공급량 예측의 필요성을 느끼게 되었고, 데이터셋 구축을 통해서 원할한 난방 및 안정적인 전력공급을 용이하게 만들고 싶었기 때문에 대시보드를 구축하게 되었습니다. 


<br><br><br>

## 사용 프로그램 
* AWS EC2
* Hadoop [3.2.4]
* Spark [3.1.3]
* Django [3.2.16]
* MySQL
* Airflow

<br><br><br>


## 서비스 구성
**데이터 파이프라인**

![불사조_아키텍처정의서](https://user-images.githubusercontent.com/105107806/199005087-cf86d392-061a-4c48-8255-efc448272e5b.png)


**데이터 수집**
* API
  * [한국은행](https://ecos.bok.or.kr/api/#/)

* 크롤링 
  * [행정안전부](https://emhu.mois.go.kr/mul/frt/biz/mul/publicItemList.do), [기상청](https://data.kma.go.kr/climate/RankState/selectRankStatisticsDivisionList.do?pgmNo=179), [국가에너지통계 종합정보시스템](https://www.kesis.net/sub/subChart.jsp?M_MENU_ID=M_M_001&S_MENU_ID=S_M_003&report_id=7060202&reportCd=7060202&chartCategory=line&minYN=2014&reportType=0)
  * BeautifulSoup, selenium 등을 사용하여 html 파싱


**데이터 처리**


**대시보드 필터 처리**
* 지역별, 기간별에 따른 공급량 및 수요량 변화 확인

**서버를 통해 배포**
* heroku 배포

<br><br><br>

## 서비스 결과
**수요량 대시보드**
![demand](https://user-images.githubusercontent.com/105107806/199034331-c4a29776-e97d-4562-a63f-eca00469b932.png)

<br>

**공급량 대시보드**
![supply](https://user-images.githubusercontent.com/105107806/199034539-c5e9d9d9-ccd7-42e4-a080-cc7fdbdfcebe.png)

<br>

**수요량 요인 대시보드**
![demand_factor](https://user-images.githubusercontent.com/105107806/199034516-a89e5faa-8efe-4eb8-86d6-63c1d53b2e51.png)

<br>

**공급량 요인 대시보드**
![supply_factor](https://user-images.githubusercontent.com/105107806/199040602-6239b12b-ee1d-4222-a92a-ae5ac1c0d034.png)