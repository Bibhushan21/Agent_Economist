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
from flask_mail import Mail, Message
from dotenv import load_dotenv
import os
from mistralai.client import MistralClient  # Import Mistral library
import PyPDF2
import os

app = Flask(__name__)

# Initialize global instances to avoid recreating them for each request
parser = None
master = None
analyzer = None

# Cache for API responses
api_cache = {}
CACHE_DURATION = 3600  # 1 hour cache duration

# Load environment variables from .env file
load_dotenv()

# Configure Flask-Mail
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')

mail = Mail(app)

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

@app.route('/send-complaint', methods=['POST'])
def send_complaint():
    try:
        data = request.json
        country = data.get('country')
        start_year = data.get('startYear')
        end_year = data.get('endYear')
        indicator = data.get('indicator')
        message = data.get('message', 'No additional message provided.')

        # Compose the email
        msg = Message('Data Issue Complaint',
                      sender='sunitasapkota047@gmail.com',  # Update with your email
                      recipients=['subedibibhushan@outlook.de'])
        msg.body = f"""
        Complaint Details:
        Country: {country}
        Start Year: {start_year}
        End Year: {end_year}
        Indicator: {indicator}
        Message: {message}
        """

        # Send the email
        mail.send(msg)

        return jsonify({'message': 'Complaint sent successfully!'}), 200
    except Exception as e:
        app.logger.error(f'Error sending complaint: {str(e)}')
        return jsonify({'message': 'Failed to send complaint.'}), 500
# Function to extract system prompt from a PDF
def extract_prompt_from_pdf(pdf_path):
    try:
        with open(pdf_path, "rb") as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            prompt_text = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    prompt_text.append(page_text.replace("\n", " "))  
            return " ".join(prompt_text).strip()
    except Exception as e:
        print(f"⚠️ Error reading system prompt: {str(e)}")
        return "You are an AI assistant. Answer questions helpfully."

# Load system prompt from the PDF
pdf_path = "Brainstorming Agent - System Prompt.pdf"  
system_prompt = extract_prompt_from_pdf(pdf_path)

print("✅ System prompt loaded successfully." if system_prompt else "⚠️ Using default system prompt.")

# System prompt message
system_message = {"role": "system", "content":
    system_prompt
    }

# Initialize chat memory (Limit history to prevent excessive memory usage)
memory = []
@app.route("/chat", methods=["POST"])
def chat():
    global memory
    data = request.get_json()

    # Validate request data
    if not data or "message" not in data or not data["message"].strip():
        return jsonify({"error": "Message cannot be empty."}), 400

    user_input = data["message"].strip()
    user_name = data.get("user_name", "User").strip()

    memory.append({"role": "user", "content": user_input})

    try:
        # Initialize Mistral client
        api_key = os.getenv('MISTRAL_API_KEY')  # Replace with your actual API key
        model = "mistral-large-latest"

        client = MistralClient(api_key=api_key)
        
        # Send chat request
        response = client.chat(
            model=model,
            messages=[system_message] + memory  # Include system prompt + chat history
        )

        ai_response = response.choices[0].message.content if response.choices else "I couldn't understand that."

        # Store AI response in memory
        memory.append({"role": "assistant", "content": ai_response})

        # Limit chat memory to the last 10 exchanges (to avoid infinite growth)
        memory = memory[-10:]

    except Exception as e:
        ai_response = f"Error: {str(e)}"

    return jsonify({"user_name": user_name, "user_message": user_input, "ai_response": ai_response})

if __name__ == '__main__':
    app.run(debug=True)