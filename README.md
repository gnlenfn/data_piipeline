# NYC TLC 데이터 적재 파이프라인 구축


### ~~유튜브 채팅 수집~~
- ~~유튜브 API 활용~~
- ~~스트리밍 방송 채팅 수집~~
- ~~Google Cloud Storage로 parquet 저장~~

## NYC Taxi & Limousine Trip data
- [데이터 출처](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Yellow Taxi, Green Taxi, For-Hire Vehicles, Commuter Van and Paratransit 네 가지 종류가 있음
- 자세한 정보는 [이 페이지](https://www.nyc.gov/site/tlc/passengers/your-ride.page) 확인
- parquet 형태로 다운 받을 수 있고 파이썬으로 2022, 2021, 2020 3년 간의 데이터를 다운 받아 Google Cloud Storage로 저장
- 

## 스파크 
- 약간의 데이터분석
- aggregation 결과를 DB로 저장 (MySQL)
- airflow로 batch job 구성

### Airflow? cron?
- Spark job batch scheduling
- spark job이 복잡하다면 airflow 사용
- 단순히 스케쥴링만 필요하다면 Airflow를 쓸 필요는 없을듯