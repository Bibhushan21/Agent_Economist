from typing import Dict, Any
import asyncio
from datetime import datetime
import aiohttp
from .base_agent import BaseAgent
from ..schemas.data_schema import DataSet, DataPoint, Metadata, DataSource

class OECDAgent(BaseAgent):
    def __init__(self):
        super().__init__("OECD")
        self.base_url = "https://stats.oecd.org/SDMX-JSON/data"
        self.indicators_mapping = {
            "gdp": "SNA/TABLE1/B1_GE",  # GDP
            "gdp_per_capita": "SNA/TABLE3/B1_GE_PC",  # GDP per capita
            "gdp growth": "SNA/TABLE1/B1_GE_GROWTH",  # Added GDP growth
            "inflation": "PRICES/CPI/CPALTT01",  # Consumer Price Index
            "unemployment": "LAB_FORCE/UNE_RATE",  # Unemployment rate
            "trade_balance": "MEI/TRD_VALUE",  # Trade balance
            "government_debt": "GOV_DEBT",  # Government debt
            "household_income": "SNA/TABLE14A/B5S14",  # Household disposable income
            "productivity": "PDB_LV/GDP_HC",  # Labor productivity
            "r_and_d": "MSTI/GERD_TOT",  # R&D expenditure
        }

    def get_available_indicators(self) -> list[str]:
        """Return list of available indicators"""
        return sorted(self.indicators_mapping.keys())

    async def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from OECD API
        """
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")

        indicator = params.get("indicator", "").lower()
        country = params.get("country")
        start_year = str(params.get("start_year", "2000"))
        end_year = str(params.get("end_year", "2023"))

        if not indicator or not country:
            raise ValueError("Both indicator and country are required parameters")

        indicator_code = self.indicators_mapping.get(indicator)
        if not indicator_code:
            available = ", ".join(self.get_available_indicators())
            raise ValueError(f"Invalid indicator. Available indicators are: {available}")

        # OECD specific endpoint construction
        url = f"{self.base_url}/{indicator_code}/{country}/all"
        query_params = {
            "startTime": start_year,
            "endTime": end_year,
            "format": "json"
        }

        print(f"OECD API URL: {url}")

        async def _fetch():
            async with self.session.get(url, params=query_params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"OECD API error: {error_text}")
                return await response.json()

        return await self.handle_retry(_fetch)

    async def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform OECD data into unified schema
        """
        try:
            # Extract data from OECD response
            dataset = raw_data.get("dataSets", [{}])[0]
            series = dataset.get("series", {})
            if not series:
                raise ValueError("No data found in OECD response")

            # Get metadata
            structure = raw_data.get("structure", {})
            dimensions = structure.get("dimensions", {}).get("observation", [])
            time_periods = next((dim for dim in dimensions if dim.get("id") == "TIME_PERIOD"), {})
            time_values = time_periods.get("values", [])

            transformed_data_points = []
            for serie_key, serie_data in series.items():
                observations = serie_data.get("observations", {})
                for time_idx, obs in observations.items():
                    if obs and obs[0] is not None:  # OECD typically puts the value in first position
                        time_period = time_values[int(time_idx)].get("id")
                        transformed_data_points.append(
                            DataPoint(
                                value=float(obs[0]),
                                year=int(time_period),
                                country_code=country,
                                country_name=self._get_country_name(structure, country),
                                additional_info={
                                    "unit": self._get_unit(structure),
                                    "frequency": self._get_frequency(structure)
                                }
                            )
                        )

            # Sort data points by year
            transformed_data_points.sort(key=lambda x: x.year)

            dataset = DataSet(
                metadata=Metadata(
                    source=DataSource.OECD,
                    indicator_code=indicator_code,
                    indicator_name=self._get_indicator_name(structure),
                    last_updated=datetime.now(),
                    frequency=self._get_frequency(structure),
                    unit=self._get_unit(structure)
                ),
                data=transformed_data_points
            )

            return dataset.dict()
        except Exception as e:
            self.logger.error(f"Error transforming OECD data: {str(e)}")
            raise

    def _get_country_name(self, structure: Dict[str, Any], country_code: str) -> str:
        """Extract country name from OECD structure"""
        try:
            dimensions = structure.get("dimensions", {}).get("series", [])
            country_dim = next((dim for dim in dimensions if dim.get("id") == "LOCATION"), {})
            country_values = country_dim.get("values", [])
            country = next((c for c in country_values if c.get("id") == country_code), {})
            return country.get("name", country_code)
        except Exception:
            return country_code

    def _get_indicator_name(self, structure: Dict[str, Any]) -> str:
        """Extract indicator name from OECD structure"""
        try:
            return structure.get("name", "")
        except Exception:
            return ""

    def _get_unit(self, structure: Dict[str, Any]) -> str:
        """Extract unit from OECD structure"""
        try:
            attributes = structure.get("attributes", {}).get("series", [])
            unit_attr = next((attr for attr in attributes if attr.get("id") == "UNIT"), {})
            return unit_attr.get("name", "")
        except Exception:
            return ""

    def _get_frequency(self, structure: Dict[str, Any]) -> str:
        """Extract frequency from OECD structure"""
        try:
            dimensions = structure.get("dimensions", {}).get("series", [])
            freq_dim = next((dim for dim in dimensions if dim.get("id") == "FREQUENCY"), {})
            return freq_dim.get("name", "yearly")
        except Exception:
            return "yearly" 