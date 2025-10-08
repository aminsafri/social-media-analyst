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
    AWS Lambda function to extract Reddit data using OAuth authentication
    """
    
    s3_client = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')
    client_id = os.environ.get('REDDIT_CLIENT_ID')
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        logger.error("Reddit credentials not set")
        return {
            'statusCode': 400,
            'body': json.dumps('REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set')
        }
    
    try:
        # Get access token
        access_token = get_reddit_access_token(client_id, client_secret)
        
        # Extract Reddit data
        reddit_data = extract_reddit_data_with_token(access_token)
        
        if len(reddit_data) == 0:
            logger.warning("No Reddit posts extracted")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"raw-data/reddit/reddit_posts_{timestamp}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(reddit_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully uploaded {len(reddit_data)} Reddit posts to {filename}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(reddit_data)} Reddit posts',
                'filename': filename
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing Reddit data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_reddit_access_token(client_id, client_secret):
    """
    Get access token from Reddit
    """
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    
    data = {
        'grant_type': 'client_credentials'
    }
    
    headers = {
        'User-Agent': 'DataWarehouse/1.0 by YourRedditUsername'
    }
    
    response = requests.post(
        'https://www.reddit.com/api/v1/access_token',
        auth=auth,
        data=data,
        headers=headers,
        timeout=10
    )
    
    response.raise_for_status()
    token_data = response.json()
    return token_data['access_token']

def extract_reddit_data_with_token(access_token):
    """
    Extract data from Reddit token
    """
    subreddits = ['technology', 'datascience', 'worldnews', 'cryptocurrency']
    all_posts = []
    
    headers = {
        'Authorization': f'bearer {access_token}',
        'User-Agent': 'DataWarehouse/1.0 by YourRedditUsername'
    }
    
    for subreddit in subreddits:
        try:
            url = f"https://oauth.reddit.com/r/{subreddit}/hot"
            params = {'limit': 25}
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            posts = data['data']['children']
            
            for post in posts:
                post_data = post['data']
                all_posts.append({
                    'post_id': post_data.get('id'),
                    'title': post_data.get('title'),
                    'author': post_data.get('author'),
                    'subreddit': post_data.get('subreddit'),
                    'score': post_data.get('score', 0),
                    'upvote_ratio': post_data.get('upvote_ratio', 0),
                    'num_comments': post_data.get('num_comments', 0),
                    'created_utc': post_data.get('created_utc'),
                    'selftext': post_data.get('selftext', '')[:500],
                    'url': post_data.get('url'),
                    'extraction_date': datetime.now().isoformat()
                })
                
            logger.info(f"Extracted {len(posts)} posts from r/{subreddit}")
                
        except Exception as e:
            logger.error(f"Error extracting from r/{subreddit}: {str(e)}")
            continue
    
    return all_posts