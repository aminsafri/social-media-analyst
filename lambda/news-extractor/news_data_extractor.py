import json
import boto3
import requests
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda function to extract News data and store in S3
    """
    
    s3_client = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')
    
    # Get API key from environment variable
    api_key = os.environ.get('NEWS_API_KEY')
    if not api_key:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'NEWS_API_KEY environment variable not set'})
        }
    
    try:
        # Extract news data
        news_data = extract_news_data(api_key)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"raw-data/news/news_articles_{timestamp}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(news_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully uploaded {len(news_data)} news articles to {filename}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(news_data)} news articles',
                'filename': filename
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing news data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def extract_news_data(api_key):
    """
    Extract data from News API
    """
    categories = ['technology', 'business', 'science']
    all_articles = []
    
    for category in categories:
        try:
            url = "https://newsapi.org/v2/top-headlines"
            params = {
                'country': 'us',
                'category': category,
                'apiKey': api_key,
                'pageSize': 20
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            articles = data.get('articles', [])
            
            for article in articles:
                all_articles.append({
                    'title': article.get('title'),
                    'description': article.get('description'),
                    'source_name': article.get('source', {}).get('name'),
                    'author': article.get('author'),
                    'published_at': article.get('publishedAt'),
                    'url': article.get('url'),
                    'category': category,
                    'country': 'us',
                    'extraction_date': datetime.now().isoformat()
                })
                
        except Exception as e:
            logger.error(f"Error extracting {category} news: {str(e)}")
            continue
    
    return all_articles