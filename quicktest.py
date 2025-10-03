# quick_test.py
import time
import requests

quick_queries = [
    "What was CO2 in construction zone yesterday?",
    "Show noise levels for S-I1",
    "What is current temperature?"  # Should fail gracefully
]

for query in quick_queries:
    start = time.time()
    try:
        response = requests.post("http://127.0.0.1:5050/query", 
                               json={"query": query}, timeout=10)
        end = time.time()
        print(f"'{query}' → {((end-start)*1000):.0f}ms → Status: {response.status_code}")
    except Exception as e:
        print(f"'{query}' → ERROR: {e}")