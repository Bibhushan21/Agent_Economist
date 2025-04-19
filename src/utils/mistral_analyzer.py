import os
from typing import Dict, Any, Optional
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage
import re
from datetime import datetime
import logging
import hashlib
import json

class MistralAnalyzer:
    def __init__(self):
        # Initialize logger
        self.logger = logging.getLogger("MistralAnalyzer")
        
        # Initialize MistralAI client
        mistral_api_key = 'g7PZ1xLVJrA6XkMPNPzh3j5ZaNdBuDUI'
        
        self.client = MistralClient(api_key=mistral_api_key)
        # Initialize cache
        self.cache = {}
        self.cache_duration = 3600  # 1 hour cache duration

    def _create_analysis_prompt(self, country: str, indicator: str, data: Dict[str, Any]) -> str:
        """Create a prompt for data analysis"""
        # Extract data points for the prompt
        data_points = data.get("data", [])
        metadata = data.get("metadata", {})
        
        # Format data points for the prompt - limit to 10 most recent points for efficiency
        formatted_data = []
        sorted_points = sorted(data_points, key=lambda x: x['year'], reverse=True)
        for point in sorted_points[:10]:  # Only use the 10 most recent data points
            formatted_data.append(f"Year: {point['year']}, Value: {point['value']}")
        
        data_str = "\n".join(formatted_data)
        
        return f"""You are an expert economic analyst. Analyze the following {indicator} data for {country} and provide:
1. A clear summary of the trends
2. Key observations and insights
3. Potential factors influencing the changes
4. Comparison with global or regional averages if relevant
5. Future outlook based on the trends

Indicator Details:
- Name: {metadata.get('indicator_name')}
- Unit: {metadata.get('unit', 'Not specified')}
- Source: World Bank

Data Points:
{data_str}

Please provide a well-structured, detailed analysis that would be helpful for understanding the economic situation of {country} based on this {indicator} data."""

    def _get_cache_key(self, country: str, indicator: str, data: Dict[str, Any]) -> str:
        """Generate a cache key for the analysis"""
        # Create a simplified version of the data for the cache key
        simplified_data = {
            "country": country,
            "indicator": indicator,
            "data_points": [{"year": p["year"], "value": p["value"]} for p in data.get("data", [])]
        }
        # Convert to JSON and hash it
        data_str = json.dumps(simplified_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if the cached data is still valid"""
        if cache_key not in self.cache:
            return False
        cached_time = self.cache[cache_key]["timestamp"]
        return (datetime.now() - cached_time).seconds < self.cache_duration

    async def analyze_data(self, country: str, indicator: str, data: Dict[str, Any]) -> str:
        """
        Analyze the data using MistralAI with caching
        """
        try:
            # Check cache first
            cache_key = self._get_cache_key(country, indicator, data)
            if self._is_cache_valid(cache_key):
                self.logger.info(f"Returning cached analysis for {country}, {indicator}")
                return self.cache[cache_key]["analysis"]
            
            # Create analysis prompt
            prompt = self._create_analysis_prompt(country, indicator, data)
            
            # Use the medium model directly for faster response
            response = self.client.chat(
                model="mistral-medium",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert economic analyst specializing in analyzing economic data and providing insightful analysis."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            analysis = response.choices[0].message.content
            
            # Cache the result
            self.cache[cache_key] = {
                "analysis": analysis,
                "timestamp": datetime.now()
            }
            
            return analysis
            
        except Exception as e:
            error_msg = str(e)
            if "API key" in error_msg.lower():
                return "Error: Invalid or missing MistralAI API key. Please check your environment variables."
            return f"Error during analysis: {error_msg}" 