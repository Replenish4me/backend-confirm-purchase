import unittest
import json
from app.handler import lambda_handler

class TestLambdaHandler(unittest.TestCase):

    def test_lambda_handler(self):
        event = {
            'body': {
                'name': 'John Doe',
            },
            'headers': {
                'Content-Type': 'application/json',
            },
        }

        context = {}

        output = lambda_handler(event, context)

        expected_output = {
            "statusCode": 200,
            "body": r'{"name": "John Doe"}',
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            }
        }

        
        self.assertEqual(output, expected_output)


