import json
from typing import Dict, Any

def lambda_handler(event: Dict[str, Dict[str, Any]], context: Dict[str, Any]):
    req_body = event.get('body') or {}
    query_params = event.get('queryStringParameters') or {}
    req_headers = event.get('headers') or {}
    path = event.get('path') or '/'

    response = {
        "statusCode": 200,
        "body": json.dumps(req_body),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
    }
    
    return response