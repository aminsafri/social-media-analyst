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
    AWS Lambda function to extract Crypto data and store in S3
    """
    
    s3_client = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')

    try:
        # Extract crypto data
        crypto_data = extract_crypto_data()
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"raw-data/crypto/crypto_prices_{timestamp}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(crypto_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully uploaded {len(crypto_data)} crypto prices to {filename}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(crypto_data)} crypto prices',
                'filename': filename
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing crypto data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def extract_crypto_data():
    """
    Extract data from CoinGecko website
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 50,
            'page': 1,
            'sparkline': False
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        crypto_data = []
        
        for coin in data:
            crypto_data.append({
                'coin_id': coin.get('id'),
                'symbol': coin.get('symbol'),
                'name': coin.get('name'),
                'current_price': coin.get('current_price'),
                'market_cap': coin.get('market_cap'),
                'market_cap_rank': coin.get('market_cap_rank'),
                'price_change_24h': coin.get('price_change_24h'),
                'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                'total_volume': coin.get('total_volume'),
                'last_updated': coin.get('last_updated'),
                'extraction_date': datetime.now().isoformat()
            })
        
        return crypto_data
        
    except Exception as e:
        logger.error(f"Error extracting crypto data: {str(e)}")
        raise