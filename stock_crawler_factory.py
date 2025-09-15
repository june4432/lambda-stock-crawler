"""
주식 크롤러 팩토리 - AWS Lambda용 메인 진입점
타입에 따라 다른 크롤러를 실행하는 팩토리 메서드
"""

import json
import os
import asyncio
from datetime import datetime
import boto3
from dotenv import load_dotenv

# .env 파일 로드 (로컬 환경에서만)
if os.path.exists('.env'):
    load_dotenv()


def factory_lambda_handler(event, context):
    """
    주식 크롤러 팩토리 Lambda 핸들러
    
    Args:
        event (dict): Lambda 이벤트 데이터
            - crawler_type: 'daily', 'quarter', 'annual'
            - s3_bucket: S3 버킷명 (선택사항)
            - delay_between_stocks: 종목 간 대기시간 (선택사항)
        context: Lambda 컨텍스트
        
    Returns:
        dict: HTTP 응답
    """
    try:
        # 환경변수 우선순위로 설정값 읽기 (환경변수 > 이벤트 파라미터 > 기본값)
        crawler_type = os.environ.get('CRAWLER_TYPE') or event.get('crawler_type') or 'daily'
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'
        delay_between_stocks = int(os.environ.get('DELAY_BETWEEN_STOCKS') or event.get('delay_between_stocks') or '2')
        
        # 디버깅을 위한 환경변수 값 출력
        print(f"🔍 환경변수 디버깅:")
        print(f"   CRAWLER_TYPE: {os.environ.get('CRAWLER_TYPE', 'None')}")
        print(f"   S3_BUCKET: {os.environ.get('S3_BUCKET', 'None')}")
        print(f"   DELAY_BETWEEN_STOCKS: {os.environ.get('DELAY_BETWEEN_STOCKS', 'None')}")
        print(f"🔍 이벤트 파라미터:")
        print(f"   crawler_type: {event.get('crawler_type', 'None')}")
        print(f"   s3_bucket: {event.get('s3_bucket', 'None')}")
        print(f"   delay_between_stocks: {event.get('delay_between_stocks', 'None')}")
        print(f"🚀 주식 크롤러 팩토리 시작 - 타입: {crawler_type}, 버킷: {s3_bucket}, 딜레이: {delay_between_stocks}초")
        
        # 이벤트에 환경변수 값들 추가
        event_with_env = event.copy()
        event_with_env['s3_bucket'] = s3_bucket
        event_with_env['delay_between_stocks'] = delay_between_stocks
        
        # 타입에 따라 분기
        if crawler_type == 'daily':
            return handle_daily_crawler(event_with_env, context)
        elif crawler_type == 'quarter':
            return handle_quarter_crawler(event_with_env, context)
        elif crawler_type == 'annual':
            return handle_annual_crawler(event_with_env, context)
        else:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'지원하지 않는 크롤러 타입: {crawler_type}',
                    'supported_types': ['daily', 'quarter', 'annual']
                }, ensure_ascii=False, indent=2)
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'팩토리 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


def handle_daily_crawler(event, context):
    """
    일간 투자정보 크롤러 실행 (PER/EPS)
    """
    print("📊 일간 투자정보(PER/EPS) 크롤러 실행")
    
    # 기존 naver_stock_invest_info_crawler 모듈 import
    from naver_stock_invest_info_crawler import lambda_handler
    
    # 해당 핸들러 실행
    return lambda_handler(event, context)


def handle_quarter_crawler(event, context):
    """
    분기별 재무정보 크롤러 실행
    """
    print("📈 분기별 재무정보 크롤러 실행")
    
    try:
        # naver_stock_invest_index_crawler 모듈 import
        from naver_stock_invest_index_crawler import run_multiple_crawler
        
        # stocks.json 파일 로드
        try:
            with open('stocks.json', 'r', encoding='utf-8') as f:
                stocks = json.load(f)
            print(f"📋 stocks.json에서 {len(stocks)}개 종목 로드 완료")
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'stocks.json 파일 로드 실패: {str(e)}'
                }, ensure_ascii=False, indent=2)
            }
        
        # 분기별 크롤링 실행
        print("🚀 분기별 재무정보 크롤링 시작")
        
        # 임시 출력 디렉토리 생성
        output_dir = "/tmp/crawl_results"
        os.makedirs(output_dir, exist_ok=True)
        
        # run_multiple_crawler 실행 (분기) - S3 업로드는 내부에서 처리됨
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'
        result = run_multiple_crawler("stocks.json", output_dir, "분기", s3_bucket)
        
        # S3 업로드 결과는 run_multiple_crawler 내부에서 처리되므로 별도 처리 불필요
        s3_upload_result = {
            "success": True,
            "message": "S3 업로드는 run_multiple_crawler 내부에서 처리됨"
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': True,
                'crawler_type': 'quarter',
                'crawl_result': result,
                's3_upload': s3_upload_result,
                'crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, ensure_ascii=False, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'분기별 크롤러 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


def handle_annual_crawler(event, context):
    """
    연간 재무정보 크롤러 실행
    """
    print("📊 연간 재무정보 크롤러 실행")
    
    try:
        # naver_stock_invest_index_crawler 모듈 import
        from naver_stock_invest_index_crawler import run_multiple_crawler
        
        # stocks.json 파일 로드
        try:
            with open('stocks.json', 'r', encoding='utf-8') as f:
                stocks = json.load(f)
            print(f"📋 stocks.json에서 {len(stocks)}개 종목 로드 완료")
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json; charset=utf-8'
                },
                'body': json.dumps({
                    'success': False,
                    'error': f'stocks.json 파일 로드 실패: {str(e)}'
                }, ensure_ascii=False, indent=2)
            }
        
        # 연간 크롤링 실행
        print("🚀 연간 재무정보 크롤링 시작")
        
        # 임시 출력 디렉토리 생성
        output_dir = "/tmp/crawl_results"
        os.makedirs(output_dir, exist_ok=True)
        
        # run_multiple_crawler 실행 (연간) - S3 업로드는 내부에서 처리됨
        s3_bucket = os.environ.get('S3_BUCKET') or event.get('s3_bucket') or 'test-stock-info-bucket'
        result = run_multiple_crawler("stocks.json", output_dir, "연간", s3_bucket)
        
        # S3 업로드 결과는 run_multiple_crawler 내부에서 처리되므로 별도 처리 불필요
        s3_upload_result = {
            "success": True,
            "message": "S3 업로드는 run_multiple_crawler 내부에서 처리됨"
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': True,
                'crawler_type': 'annual',
                'crawl_result': result,
                's3_upload': s3_upload_result,
                'crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, ensure_ascii=False, indent=2)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'success': False,
                'error': f'연간 크롤러 실행 중 오류: {str(e)}'
            }, ensure_ascii=False, indent=2)
        }


if __name__ == "__main__":
    print("🏭 주식 크롤러 팩토리 - 로컬 테스트")
    print("=" * 50)
    
    # 테스트 이벤트 (환경변수 사용)
    test_events = [
        # {
        #     'crawler_type': os.environ.get('CRAWLER_TYPE', 'daily'),
        #     's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
        #     'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        # },
        {
           'crawler_type': 'quarter',
           's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
           'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        }
        # ,
        # {
        #    'crawler_type': 'annual',
        #    's3_bucket': os.environ.get('S3_BUCKET', 'test-stock-info-bucket'),
        #    'delay_between_stocks': int(os.environ.get('DELAY_BETWEEN_STOCKS', '2'))
        # }
    ]
    
    for test_event in test_events:
        print(f"\n🧪 테스트: {test_event['crawler_type']}")
        result = factory_lambda_handler(test_event, None)
        print(f"결과 상태코드: {result['statusCode']}")
        
        if result['statusCode'] == 200:
            body = json.loads(result['body'])
            print(f"✅ 성공: {body.get('success', False)}")
        else:
            print(f"❌ 실패: {result['statusCode']}")
