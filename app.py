from flask import Flask, request, jsonify, render_template
import asyncio
from src.agents.master_agent import MasterAgent
from main import QueryParser
from src.utils.mistral_analyzer import MistralAnalyzer
from src.utils.visual_representation import prepare_visual_data
from typing import Dict, Any
import hashlib
import json
from datetime import datetime, timedelta

app = Flask(__name__)

# Initialize global instances to avoid recreating them for each request
parser = None
master = None
analyzer = None

# Cache for API responses
api_cache = {}
CACHE_DURATION = 3600  # 1 hour cache duration

def get_cache_key(endpoint: str, data: Dict[str, Any]) -> str:
    """Generate a cache key for the API request"""
    # Create a simplified version of the data for the cache key
    simplified_data = {k: v for k, v in data.items() if k != 'timestamp'}
    # Convert to JSON and hash it
    data_str = json.dumps(simplified_data, sort_keys=True)
    return f"{endpoint}:{hashlib.md5(data_str.encode()).hexdigest()}"

def is_cache_valid(cache_key: str) -> bool:
    """Check if the cached data is still valid"""
    if cache_key not in api_cache:
        return False
    cached_time = api_cache[cache_key]["timestamp"]
    return (datetime.now() - cached_time).seconds < CACHE_DURATION

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/mcp/fetch', methods=['POST'])
def mcp_fetch():
    try:
        data = request.json
        query = data.get('query')
        fetch_only = data.get('fetch_only', False)  # New parameter to indicate we only want raw data
        app.logger.info(f'Received query: {query}, fetch_only: {fetch_only}')
        
        # Check cache first
        cache_key = get_cache_key('fetch', {'query': query, 'fetch_only': fetch_only})
        if is_cache_valid(cache_key):
            app.logger.info('Returning cached response')
            return jsonify(api_cache[cache_key]["response"])
        
        # Initialize the parser and master agent if not already initialized
        global parser, master
        if parser is None:
            parser = QueryParser()
        if master is None:
            master = MasterAgent()

        # Parse the query and fetch data
        params = asyncio.run(parser.parse_query(query))
        app.logger.info(f'Parsed parameters: {params}')
        
        # If fetch_only is True, we only need the raw data without analysis
        if fetch_only:
            result = asyncio.run(master.fetch_data_only(params))
        else:
            result = asyncio.run(master.fetch_with_retry(params))

        # Serialize the result to a JSON-serializable format
        result_dict = result.model_dump()
        
        # Cache the response
        api_cache[cache_key] = {
            "response": result_dict,
            "timestamp": datetime.now()
        }

        app.logger.info('Data fetched successfully')
        return jsonify(result_dict)
    except Exception as e:
        app.logger.error(f'Error in mcp_fetch: {str(e)}')
        return jsonify({"error": str(e)}), 500

@app.route('/mcp/analyze', methods=['POST'])
def mcp_analyze():
    try:
        data = request.json
        country = data.get('country')
        indicator = data.get('indicator')
        dataset = data.get('dataset')
        
        # Check cache first
        cache_key = get_cache_key('analyze', {
            'country': country,
            'indicator': indicator,
            'dataset': dataset
        })
        if is_cache_valid(cache_key):
            app.logger.info('Returning cached analysis')
            return jsonify(api_cache[cache_key]["response"])
        
        app.logger.info(f'Received analysis request for {country}, {indicator}')

        # Initialize the analyzer if not already initialized
        global analyzer
        if analyzer is None:
            analyzer = MistralAnalyzer()

        # Perform analysis
        analysis_result = asyncio.run(analyzer.analyze_data(country, indicator, dataset))
        
        # Cache the response
        response = {"analysis": analysis_result}
        api_cache[cache_key] = {
            "response": response,
            "timestamp": datetime.now()
        }

        app.logger.info('Analysis completed successfully')
        return jsonify(response)
    except Exception as e:
        app.logger.error(f'Error in mcp_analyze: {str(e)}')
        return jsonify({"error": str(e)}), 500

@app.route('/mcp/visualize', methods=['POST'])
def mcp_visualize():
    try:
        data = request.json
        merged_data = data.get('merged_data')
        
        # Check cache first
        cache_key = get_cache_key('visualize', {'merged_data': merged_data})
        if is_cache_valid(cache_key):
            app.logger.info('Returning cached visualization data')
            return jsonify(api_cache[cache_key]["response"])
        
        app.logger.info('Preparing data for visualization')

        # Prepare data for visualization
        visual_data = prepare_visual_data(merged_data)
        
        # Cache the response
        api_cache[cache_key] = {
            "response": visual_data,
            "timestamp": datetime.now()
        }

        app.logger.info('Data prepared for visualization')
        return jsonify(visual_data)
    except Exception as e:
        app.logger.error(f'Error in mcp_visualize: {str(e)}')
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)