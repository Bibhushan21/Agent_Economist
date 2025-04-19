from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import aiohttp
import asyncio
from datetime import datetime, timedelta
import logging
from ..schemas.data_schema import DataPoint

# Define conversion factors globally
conversion_factors = {
    "millions": 1e-6,
    "billions": 1e-3,
    "trillions": 1.0  # Already in trillions
}

class SharedState:
    """A simple shared state to store units determined by agents."""
    un_unit: str = "unknown"
    wb_unit: str = "unknown"

    @classmethod
    def set_un_unit(cls, unit: str):
        cls.un_unit = unit

    @classmethod
    def set_wb_unit(cls, unit: str):
        cls.wb_unit = unit

    @classmethod
    def get_un_unit(cls) -> str:
        return cls.un_unit

    @classmethod
    def get_wb_unit(cls) -> str:
        return cls.wb_unit

class BaseAgent(ABC):
    def __init__(self, name: str, cache_duration: int = 3600):
        self.name = name
        self.cache = {}
        self.cache_duration = cache_duration
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(name)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @abstractmethod
    async def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Abstract method to fetch data from the specific data source
        """
        pass

    @abstractmethod
    async def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Abstract method to transform data into the unified schema
        """
        pass

    def get_cache_key(self, params: Dict[str, Any]) -> str:
        """
        Generate a cache key from the parameters
        """
        return f"{self.name}_{str(sorted(params.items()))}"

    def is_cache_valid(self, cache_key: str) -> bool:
        """
        Check if the cached data is still valid
        """
        if cache_key not in self.cache:
            return False
        cached_time = self.cache[cache_key]["timestamp"]
        return (datetime.now() - cached_time).seconds < self.cache_duration

    async def get_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main method to get data with caching and error handling
        """
        cache_key = self.get_cache_key(params)
        
        if self.is_cache_valid(cache_key):
            self.logger.info(f"Returning cached data for {cache_key}")
            return self.cache[cache_key]["data"]

        try:
            raw_data = await self.fetch_data(params)
            transformed_data = await self.transform_data(raw_data)
            
            self.cache[cache_key] = {
                "data": transformed_data,
                "timestamp": datetime.now()
            }
            
            return transformed_data
        except Exception as e:
            self.logger.error(f"Error in {self.name}: {str(e)}")
            raise

    async def handle_retry(self, func, max_retries: int = 3, delay: int = 1):
        """
        Retry mechanism for failed requests
        """
        for attempt in range(max_retries):
            try:
                return await func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff 

    def convert_to_unit(self, data_points: list[DataPoint], target_unit: str) -> list[DataPoint]:
        """
        Convert data points to the target unit.
        """
        # Get the conversion factor for the target unit
        target_conversion_factor = conversion_factors.get(target_unit.lower(), 1.0)

        # Convert each data point
        converted_data_points = []
        for point in data_points:
            # Determine the current unit conversion factor
            current_unit = point.additional_info.get("unit", "trillions").lower()
            current_conversion_factor = conversion_factors.get(current_unit, 1.0)

            # Convert value to target unit
            converted_value = point.value * (current_conversion_factor / target_conversion_factor)
            converted_data_points.append(DataPoint(
                value=converted_value,
                year=point.year,
                country_code=point.country_code,
                country_name=point.country_name,
                additional_info=point.additional_info
            ))

        return converted_data_points 

    def determine_unit(self, value: float) -> str:
        """
        Determine the unit of the data based on the number of digits.
        """
        num_digits = len(str(int(value)))
        if num_digits <= 6:
            return "units"
        elif num_digits <= 9:
            return "millions"
        elif num_digits <= 12:
            return "billions"
        else:
            return "trillions" 