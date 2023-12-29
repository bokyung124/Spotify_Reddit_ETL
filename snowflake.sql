CREATE DATABASE spotify;

CREATE SCHEMA spotify.raw_data;

-- raw data table 생성
CREATE TABLE spotify.raw_data.album (
    album varchar(100),
    album_id varchar(100) primary key,
    "date" date,
    artist varchar(100),
    artist_id varchar(100)
);

CREATE TABLE spotify.raw_data.genres(
    album_id varchar(100),
    genres text,
    FOREIGN KEY (album_id) REFERENCES spotify.raw_data.album(album_id)
);

CREATE TABLE spotify.raw_data.reddit(
    title varchar(500),
    content text,
    sentiment float
);

-- 앨범 정보와 장르를 따로 저장하기 위한 임시 테이블
CREATE TEMPORARY TABLE temp_album (
    album varchar(100),
    album_id varchar(100) primary key,
    "date" varchar(100),
    artist varchar(100),
    artist_id varchar(100),
    genres text
);

COPY INTO temp_album
FROM 's3://spotify-etl-bk/album.parquet'
credentials=(AWS_KEY_ID='***' AWS_SECRET_KEY='***')
FILE_FORMAT = (type='parquet')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

INSERT INTO spotify.raw_data.album (album, album_id, "date", artist, artist_id)
SELECT album, album_id, "date", artist, artist_id FROM temp_album;

INSERT INTO spotify.raw_data.genres(album_id, genres)
SELECT album_id, value::STRING FROM temp_album,
LATERAL FLATTEN(INPUT => SPLIT(genres, ','));

COPY INTO spotify.raw_data.reddit
FROM 's3://spotify-etl-bk/reddit.parquet'
credentials=(AWS_KEY_ID='***' AWS_SECRET_KEY='***')
FILE_FORMAT = (type='parquet')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';