# 가상의 데이터 인프라 구축하기
## 목적
채용 관련 광고를 집행한 뒤 성과를 측정하기 위함

## raw_data
### database
1. user (사용자 정보)
	1. id
	2. 가입일
2. referral (utm 정보가 있는 경우 트래킹을 위하여 생성되는 row)
	1. user_id
	2. utm_source
	3. utm_media
	4. utm_campaign
	5. ip_address
	6. 생성 시간
3. 지원서 (사용자가 회사의 채용공고에 지원할 때 생성되는 지원서)
	1. user_id
	2. 채용 공고 id
	3. 연차
	4. 상태 (지원함, 채용됨, 거절됨 등등)
	5. 각 상태별 시간
	6. 최소 희망 연봉
4. 채용 공고
	1. 회사 id
	2. 연차 범위
	3. 직군
	4. 연봉 범위
	5. 내용

### Ad data api
1. 집행 내역 (상세 데이터는 확인 필요)
	1. 캠페인
	2. 비용
	3. 기간
	4. 매체명?
	5. 고유 아이디

### Logs
1. Action logs
	1. request_id (request마다 고유하게 생성되는 값)
	2. user_id
	3. action (방문한 controller action// e.g. https://host.com/채용공고/1 )
	4. request_ip
	
## summary table
1. DAU per campaign/source
2. 일별 작성된 지원서 per campaign/source
3. 광고를 본 후 N일간 확인한 채용 공고
4. 지원서를 작성한 사용자가 N일간 접한 광고 

## 고민
1. 모든 production log를 aws cloudwatch로 수집하고 있는데, CloudWatch 로 들어온 log를 kinesis firehose -> redshift로 load 하려고 하는데 적절한 선택인지 궁금합니다.
2. 위의 경우 firehose가 airflow와 동일한 역할을 수행하지만, 들어오는 소스가 실시간 스트림이기 때문에 생기는 차이인가요?
3. 각 광고 매체별로 제공해주는 정보의 경우 집행 내역 말고도 방문자수 등 다양하게 있을 것으로 생각되는데, 어떻게 내부 데이터와 통합해서 분석에 이용하면 좋을지 아이디어가 잘 떠오르질 않습니다. 

