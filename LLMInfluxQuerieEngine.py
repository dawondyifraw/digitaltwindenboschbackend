#!/usr/bin/env python3

from flask import Flask, request, jsonify
import ExplainerFinal as LLMResponse
import logging 
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # This will allow all domains by default


# Mock LLM function (replace with your actual LLM integration)
def query_llm(user_input):
    # Replace this with your LLM logic (e.g., OpenAI API, Hugging Face, etc.)
    answer = LLMResponse.handle_query(user_input)
    return f"Response to: {answer}"

@app.route('/query', methods=['POST'])
def handle_query():
    data = request.json
    user_input = data.get('query')
    logging.log(logging.INFO, f"Query {user_input}")
    if not user_input:
        return jsonify({"error": "No query provided"}), 400
    
    response = query_llm(user_input)
    return jsonify({"response": response})

if __name__ == '__main__':
    app.run(debug=True,port=5050)