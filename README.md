# 도시가스 수요량 및 공급량 예측을 위한 데이터셋 구축
> 도시가스 수요 및 공급 예측을 위한 데이터셋 대시보드를 구축하였습니다.<br>
완성된 대시보드 웹 페이지는 [여기](http://teamphoenix.site/)서 확인 가능합니다.<br>
프로젝트 기간: 22.07.28 ~ 2022.08.17

<br>

## 프로젝트 목표
* 한국가스공사 대상 천연가스 수입량 결정에 필요한 데이터셋 제공
* 도시가스 지역관리소 대상 공급 괍리에 필요한 데이터셋 제공
* 정확한 에너지 수요 예측을 통한 직·간접적인 경제 사회 비용 절감


<br>

## 활용 데이터
|  no  |         내용         |          출처          |    형식/방식     |
|:----:|:------------------:|:--------------------:|:------------:|
|  1   |    월별 도시가스 공급실적    | [한국도시가스협회][한국도시가스협회] |   XLS/FILE   |
|  2   |  월간 시도별 도시가스 판매현황  |  [공공데이터포털][공공데이터포털]  |   CSV/FILE   |
|  3   |  한국지역난방공사 난방지수 정보  |  [공공데이터포털][공공데이터포털]  |   CSV/FILE   |
|  4   | 가스(LNG) 수급 동향 및 전망 |  [산업통상자원부][산업통상자원부]  |   CSV/FILE   |
|  5   |      생산자물가지수       |     [한국은행][한국은행]     |   JSON/API   |
|  6   |       수입물가지수       |     [한국은행][한국은행]     |   JSON/API   |
|  7   |       수입물량지수       |     [한국은행][한국은행]     |   JSON/API   |
|  8   |       수입금액지수       |     [한국은행][한국은행]     |   JSON/API   |
|  9   |       도시가스요금       |     [한국은행][한국은행]     | CSV/CRAWLING |
|  10  |     도시가스 수요가 수      |  [국가에너지통계][국가에너지통계]  | CSV/CRAWLING |
|  11  |  지역별 기온분석 월 자료   | [기상자료개방포털][기상자료개방포털] | CSV/CRAWLING |


[한국도시가스협회]: http://www.citygas.or.kr/
[공공데이터포털]: https://www.data.go.kr/
[산업통상자원부]: http://www.motie.go.kr/www/main.do/
[한국은행]: https://ecos.bok.or.kr/api/#/
[국가에너지통계]: http://www.kesis.net/main/main.jsp/
[기상자료개방포털]: https://data.kma.go.kr/cmmn/main.do/

<br>

## 아키텍처 및 기술 스택

![불사조_아키텍처정의서](https://user-images.githubusercontent.com/105107806/199005087-cf86d392-061a-4c48-8255-efc448272e5b.png)

* AWS EC2
* Hadoop [3.2.4]
* Spark [3.1.3]
* Django [3.2.16]
* MySQL
* Airflow

<br>

## 데이터 파이프라인 구축 상세
**데이터 수집 및 적재**
* BeautifulSoup, selenium 등을 사용하여 html 파싱 후 hadoop 적재

**데이터 처리**
* pyspark 활용하여 총 11종류, 34개의 raw data를 최종 ERD에 맞게 가공 후 mySQL 저장

**서비스 페이지 구현**
* Django 이용하여 서비스 페이지 제작
* 지역별, 기간별에 따른 공급량 및 수요량 변화 확인

**자동화**
* 지속적인 업데이트가 필요한 ETL 과정[데이터 수집 -> 하둡 적재 -> 스파크 처리 -> MySQL 적재]을 airflow로 스케줄링하여 자동화

**서비스 배포**
* AWS EC2를 통한 서비스 구축 후 NGINX·uWSGI를 통한 배포 

<br>

## 서비스 화면
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
