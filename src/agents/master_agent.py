import asyncio
from typing import Dict, Any, List, Type, Optional
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import hashlib
import json

from .base_agent import BaseAgent
from .world_bank_agent import WorldBankAgent
from .imf_agent import IMFAgent
from .oecd_agent import OECDAgent
from .un_agent import UNAgent
from ..schemas.data_schema import AggregatedDataResponse, DataSet, Metadata, DataSource, DataPoint
from ..utils.mistral_analyzer import MistralAnalyzer

load_dotenv()

class MasterAgent:
    def __init__(self):
        self.logger = logging.getLogger("MasterAgent")
        self.agents: Dict[str, Type[BaseAgent]] = {
            "world_bank": WorldBankAgent,
            "imf": IMFAgent,
            "oecd": OECDAgent,
            "un": UNAgent,
        }
        
        # Initialize cache
        self.cache = {}
        self.cache_duration = 3600  # 1 hour cache duration
        
        try:
            self.analyzer = MistralAnalyzer()
            self.logger.info("✅ Initialized MistralAnalyzer")
        except Exception as e:
            self.logger.error(f"⚠️ Failed to initialize MistralAnalyzer: {e}")
            self.analyzer = None

    def _get_cache_key(self, params: Dict[str, Any]) -> str:
        """Generate a cache key for the parameters"""
        # Create a simplified version of the params for the cache key
        simplified_params = {
            "indicator": params.get("indicator", ""),
            "country": params.get("country", ""),
            "start_year": params.get("start_year", "2000"),
            "end_year": params.get("end_year", "2023")
        }
        # Convert to JSON and hash it
        params_str = json.dumps(simplified_params, sort_keys=True)
        return hashlib.md5(params_str.encode()).hexdigest()

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if the cached data is still valid"""
        if cache_key not in self.cache:
            return False
        cached_time = self.cache[cache_key]["timestamp"]
        return (datetime.now() - cached_time).seconds < self.cache_duration

    async def _merge_datasets(self, datasets: List[DataSet]) -> DataSet:
        """
        Merge datasets from all sources into a single dataset.
        """
        merged_data_points = {}

        # Collect all unique years from the datasets
        all_years = set()
        for dataset in datasets:
            for data_point in dataset.data:
                all_years.add(data_point.year)

        # Prioritize World Bank data and fill missing years with IMF data
        for year in sorted(all_years):
            wb_data_point = next((dp for ds in datasets if ds.metadata.source == DataSource.WORLD_BANK for dp in ds.data if dp.year == year), None)
            imf_data_point = next((dp for ds in datasets if ds.metadata.source == DataSource.IMF for dp in ds.data if dp.year == year), None)

            if wb_data_point:
                # Use World Bank data if available
                merged_data_points[year] = wb_data_point
            elif imf_data_point:
                # Use IMF data if World Bank data is not available
                merged_data_points[year] = imf_data_point

        # Create a merged dataset with normalized unit
        merged_dataset = DataSet(
            metadata=Metadata(
                source=DataSource.WORLD_BANK,  # Use a generic source
                indicator_code="merged",
                indicator_name="Merged Data",
                last_updated=datetime.now(),
                frequency="yearly",
                unit="trillions"  # Set the unit to trillions
            ),
            data=list(merged_data_points.values())
        )

        return merged_dataset

    async def fetch_data_only(self, params: Dict[str, Any]) -> AggregatedDataResponse:
        """
        Fetch only raw data without performing analysis.
        This is used for progressive loading where we want to display data immediately.
        """
        # Check cache first
        cache_key = self._get_cache_key(params)
        if self._is_cache_valid(cache_key):
            self.logger.info(f"Returning cached data for {params.get('indicator')}, {params.get('country')}")
            return self.cache[cache_key]["response"]
        
        # Only fetch from agents that support the requested indicator
        indicator = params.get("indicator", "").lower()
        tasks = []
        async with asyncio.TaskGroup() as group:
            for agent_name, agent_class in self.agents.items():
                # Skip agents that don't support this indicator
                agent = agent_class()
                if hasattr(agent, "get_available_indicators") and indicator in agent.get_available_indicators():
                    tasks.append(
                        group.create_task(
                            self._fetch_from_agent(agent_class, params)
                        )
                    )

        results = [task.result() for task in tasks if not task.cancelled()]

        # Merge datasets
        merged_dataset = await self._merge_datasets([DataSet(**result) for result in results if "error" not in result])

        # Create response without analysis
        response = AggregatedDataResponse(
            query_params=params,
            timestamp=datetime.now(),
            datasets=[merged_dataset],
            status="completed" if not any("error" in result for result in results) else "partial_success",
            error_summary={result["agent"]: [result["error"]] for result in results if "error" in result},
            analyses={}  # Empty analyses since we're not performing analysis
        )
        
        # Cache the response
        self.cache[cache_key] = {
            "response": response,
            "timestamp": datetime.now()
        }

        return response

    async def fetch_all_data(self, params: Dict[str, Any]) -> AggregatedDataResponse:
        """
        Fetch data from all available agents concurrently and merge the results.
        """
        # Check cache first
        cache_key = self._get_cache_key(params)
        if self._is_cache_valid(cache_key):
            self.logger.info(f"Returning cached data for {params.get('indicator')}, {params.get('country')}")
            return self.cache[cache_key]["response"]
        
        # Only fetch from agents that support the requested indicator
        indicator = params.get("indicator", "").lower()
        tasks = []
        async with asyncio.TaskGroup() as group:
            for agent_name, agent_class in self.agents.items():
                # Skip agents that don't support this indicator
                agent = agent_class()
                if hasattr(agent, "get_available_indicators") and indicator in agent.get_available_indicators():
                    tasks.append(
                        group.create_task(
                            self._fetch_from_agent(agent_class, params)
                        )
                    )

        results = [task.result() for task in tasks if not task.cancelled()]

        # Merge datasets
        merged_dataset = await self._merge_datasets([DataSet(**result) for result in results if "error" not in result])

        # Analyze merged data
        analyses = {}
        if self.analyzer:
            try:
                analysis = await self.analyzer.analyze_data(
                    country=params.get("country", "Unknown"),
                    indicator=params.get("indicator", "Unknown"),
                    data=merged_dataset.dict()
                )
                analyses["merged"] = analysis
            except Exception as e:
                self.logger.error(f"Error analyzing merged data: {str(e)}")
                analyses["error"] = f"Analysis failed: {str(e)}"
        else:
            analyses["error"] = "Analyzer not initialized."

        response = AggregatedDataResponse(
            query_params=params,
            timestamp=datetime.now(),
            datasets=[merged_dataset],
            status="completed" if not any("error" in result for result in results) else "partial_success",
            error_summary={result["agent"]: [result["error"]] for result in results if "error" in result},
            analyses=analyses
        )
        
        # Cache the response
        self.cache[cache_key] = {
            "response": response,
            "timestamp": datetime.now()
        }

        return response

    async def _fetch_from_agent(self, agent_class: Type[BaseAgent], params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from a single agent with error handling
        """
        try:
            async with agent_class() as agent:
                return await agent.get_data(params)
        except Exception as e:
            self.logger.error(f"Error fetching data from {agent_class.__name__}: {str(e)}")
            return {
                "error": str(e),
                "agent": agent_class.__name__
            }

    async def fetch_with_retry(self, params: Dict[str, Any], max_retries: int = 2) -> AggregatedDataResponse:
        """
        Fetch data with retry mechanism
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return await self.fetch_all_data(params)
            except Exception as e:
                last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                await asyncio.sleep(1)  # Reduced backoff time

        raise Exception(f"Failed after {max_retries} attempts. Last error: {str(last_error)}")

    def validate_params(self, params: Dict[str, Any]) -> bool:
        """
        Validate input parameters
        """
        required_fields = ["indicator", "country"]
        return all(field in params for field in required_fields)

    def get_available_indicators(self) -> Dict[str, List[str]]:
        """
        Get available indicators from all agents
        """
        indicators = {}
        for agent_name, agent_class in self.agents.items():
            try:
                agent = agent_class()
                if hasattr(agent, "get_available_indicators"):
                    indicators[agent_name] = agent.get_available_indicators()
            except Exception as e:
                self.logger.error(f"Error getting indicators from {agent_name}: {str(e)}")
                indicators[agent_name] = []
        return indicators 