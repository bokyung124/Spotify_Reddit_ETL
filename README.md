# Spotify_Reddit_ETL
KDT DE-Dev Course Toy Project

## spotify.ipynb

Spotify API를 사용하여 최근 발매된 앨범 정보와 해당 아티스트의 장르 정보를 가져와 parquet 혹은 JSON 형태로 S3에 업로드합니다.

## reddit.ipynb

Reddit 사이트의 특정 커뮤니티에서 특정 키워드가 들어간 게시글의 제목과 내용을 가져옵니다.      
textblob 라이브러리를 이용해 게시글 내용을 0에서 5사이로 감정분석한 뒤 합친 파일을 parquet 형태로 S3에 업로드합니다.

## snowflake.sql

S3에 있는 파일을 snowflake로 벌크 업데이트 하는 코드입니다.     
spotify에서 한 테이블에 있던 앨범 정보와 장르를 분리하여 1:N 관계의 테이블로 구성하였습니다.       
원래는 snowflake를 이용하여 superset으로 시각화를 하려고 했지만, snowflake와 superset 연결에 문제가 생겨 이 부분은 추후에 다시 시도할 예정입니다.

## lambda_function.py

spotify와 reddit API로 데이터를 가져오고 이를 S3에 적재하는 과정을 AWS Lambda 함수로 구현한 코드입니다.      
이 부분에서도 numpy import 오류가 발생해 테스트를 진행하지 못했지만, 이 부분도 수정할 예정입니다.
