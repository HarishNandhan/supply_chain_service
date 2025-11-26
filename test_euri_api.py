"""Test Euri AI API connection"""
import requests
import os
from dotenv import load_dotenv

load_dotenv('configs/.env')

EURI_API_KEY = os.getenv("EURI_API_KEY")
EURI_MODEL = os.getenv("EURI_MODEL_NAME", "gpt-4.1-nano")

# Try different possible endpoints
endpoints = [
    "https://api.euri.ai/v1/chat/completions",
    "https://euri.ai/api/v1/chat/completions",
    "https://api.euriai.com/v1/chat/completions",
    "https://api.openai.com/v1/chat/completions",  # If Euri uses OpenAI proxy
]

headers = {
    "Authorization": f"Bearer {EURI_API_KEY}",
    "Content-Type": "application/json"
}

payload = {
    "model": EURI_MODEL,
    "messages": [
        {"role": "user", "content": "Say 'Hello, I am working!' in one sentence."}
    ],
    "max_tokens": 50
}

print(f"Testing Euri AI with key: {EURI_API_KEY[:20]}...")
print(f"Model: {EURI_MODEL}\n")

for endpoint in endpoints:
    print(f"Trying endpoint: {endpoint}")
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ SUCCESS!")
            print(f"Response: {result}")
            print(f"\nWorking endpoint: {endpoint}")
            break
        else:
            print(f"❌ Failed: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Error: {str(e)[:100]}")
    print("-" * 80)
