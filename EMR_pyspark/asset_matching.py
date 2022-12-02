from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

sc.install_pypi_package("pandas==0.25.1")
sqlContext = SQLContext(sc)
bucket = "pdata-storage"

import builtins
import pandas as pd
import numpy as np
import difflib
import re

# 수집 데이터 불러오기
TBRY = sqlContext.table("ecs_etl.TBRY").select("marketplace", "product_src", "product_href", "product_title",
                                               "product_price", "product_seller")

# 텍스트 유사도 함수에 사용할 Official product list 불러오기
TBRY_OP = sqlContext.table("ecs_etl.official_product_list").select("product_name").dropDuplicates()


# 에셋 매칭 : 자카드 유사도
def asset_match_kr_Jaccard(input_string, official_product_titles):
    # 유사도 지정
    t_value = 0.6
    candidates = {}

    for official_product_title in official_product_titles:
        intersection_cardinality = len(set.intersection(*[set(official_product_title), set(input_string)]))
        union_cardinality = len(set.union(*[set(official_product_title), set(input_string)]))
        similarity = intersection_cardinality / float(union_cardinality)

        # similarity가 t_value 이상이면, candidates(딕셔너리)에 공식 상품명(Key), similarity(Value)를 넣는다.
        if similarity >= t_value:
            candidates[official_product_title] = similarity

    # candidates(딕셔너리)가 비어있지 않을 경우, 가장 큰 similarity(Value)를 가진 공식 상품명(Key)을 반환한다.
    if len(candidates) > 0:
        return builtins.max(candidates, key=candidates.get)
    return None


# 텍스트 클렌징
def clean_text_kr(text):
    # ! 특정 단어 지정 삭제 !
    exclude_tokens = ['토리버치', '여성용', '여성', '우먼', '여자', '팝니다', '판매']

    # 괄호 안의 모든 문자, 특수문자, 감정 표현 대용 및 무관한 낱말 제거
    regex_tokens = ['\[[^)]*\]', '\([^)]*\)', '[a-zA-z0-9]',
                    '[\{\}\[\]\/?.,;:|\)*~`!^\-_+<>@\#$%&\\\=\(\'\"\♥\♡\ㅋ\ㅠ\ㅜ\ㄱ\ㅎ\ㄲ\ㅡ]']

    cleaned_text = text

    for regex_token in regex_tokens:
        cleaned_text = re.sub(pattern=regex_token, repl='', string=cleaned_text)

    for exclude_token in exclude_tokens:
        cleaned_text = cleaned_text.replace(exclude_token, "")

    return cleaned_text.strip()


# Asset Matching 실행
TBRY_OFFICIAL_P_TITLE = [data[0] for data in TBRY_OP.select('product_name').collect()]
TBRY_P_TITLE = [data[0] for data in TBRY.select('product_title').collect()]

df_product_title_match = []

for p_title in TBRY_P_TITLE:
    df_product_title_match.append(asset_match_kr_Jaccard(clean_text_kr(p_title), TBRY_OFFICIAL_P_TITLE))

# 해당 결과를 기존 데이터 프레임의 새로운 column으로 추가
TBRY = TBRY.repartition(1).withColumn("asset_match",
                                      udf(lambda id: df_product_title_match[id])(monotonically_increasing_id()))

# TempView 저장 및 Bucket에 저장
# TBRY.createOrReplaceTempView("TBRY")
TBRY.write.format('csv').mode('overwrite').option("header", "true").save('path')

# JDBC
TBRY.write.format('jdbc').options(
    url='jdbc:redshift://class-dm.0000000.ap-northeast-2.redshift.amazonaws.com:5439/dev',
    driver='com.amazon.redshift.jdbc42.Driver',
    dbtable='dev.analysis_tb.silver_data',
    user='00000',
    password='00000').mode('overwrite').save()
