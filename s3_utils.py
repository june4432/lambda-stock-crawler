"""
S3 업로드 공통 유틸리티 함수들
"""

import boto3
import os
from datetime import datetime


def upload_file_to_s3(file_path, bucket_name, s3_key):
    """
    파일을 S3에 업로드
    
    Args:
        file_path (str): 업로드할 파일 경로
        bucket_name (str): S3 버킷명
        s3_key (str): S3 객체 키 (경로)
        
    Returns:
        dict: 업로드 결과
    """
    try:
        s3_client = boto3.client('s3')
        
        # 파일 업로드
        s3_client.upload_file(
            file_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                'ContentType': 'text/csv; charset=utf-8'
            }
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        file_size = os.path.getsize(file_path)
        
        print(f"✅ 파일 S3 업로드 성공: {s3_url}")
        
        return {
            "success": True,
            "s3_url": s3_url,
            "bucket": bucket_name,
            "key": s3_key,
            "size": file_size
        }
        
    except Exception as e:
        print(f"❌ S3 업로드 실패: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def upload_csv_content_to_s3(csv_content, bucket_name, s3_key):
    """
    CSV 문자열 데이터를 S3에 업로드
    
    Args:
        csv_content (str): CSV 형태의 문자열 데이터
        bucket_name (str): S3 버킷명
        s3_key (str): S3 객체 키 (경로)
        
    Returns:
        dict: 업로드 결과
    """
    try:
        s3_client = boto3.client('s3')
        
        # CSV 데이터를 UTF-8 BOM과 함께 bytes로 변환 (한글 깨짐 방지)
        csv_bytes = csv_content.encode('utf-8-sig')
        
        # S3에 업로드
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_bytes,
            ContentType='text/csv; charset=utf-8'
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ CSV 파일 S3 업로드 성공: {s3_url}")
        
        return {
            "success": True,
            "s3_url": s3_url,
            "bucket": bucket_name,
            "key": s3_key,
            "size": len(csv_bytes)
        }
        
    except Exception as e:
        print(f"❌ S3 업로드 실패: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def generate_s3_key(period_type, data_year=None, data_month=None, data_day=None):
    """
    S3 키 생성 - 크롤링된 데이터의 연도/월 사용
    
    Args:
        period_type (str): "daily", "quarter", "annual"
        data_year (str): 크롤링된 데이터의 연도
        data_month (str): 크롤링된 데이터의 월
        data_day (str): 크롤링된 데이터의 일 (daily용)
    
    Returns:
        str: S3 키
    """
    if data_year and data_month:
        year = data_year
        mm = data_month
        if data_day:
            mmdd = data_month.zfill(2) + data_day.zfill(2)
        else:
            mmdd = data_month.zfill(2) + "01"  # 기본값
    else:
        # 폴백: 현재 날짜 사용
        current_time = datetime.now()
        year = current_time.strftime("%Y")
        mm = current_time.strftime("%m")
        mmdd = current_time.strftime("%m%d")
    
    if period_type == "daily":
        s3_key = f"l0/ver=1/sys=naver/loc=common/table=external_metrics_stock/year={year}/mmdd={mmdd}/stock_invest_info.csv"
    elif period_type in ["quarter", "annual"]:
        s3_key = f"l0/ver=1/sys=naver/loc=common/table=external_metrics_{period_type}/year={year}/mm={mm}/stock_invest_info.csv"
    else:
        s3_key = f"l0/ver=1/sys=naver/loc=common/table=external_metrics_{period_type}/year={year}/mm={mm}/data.csv"
    
    return s3_key
