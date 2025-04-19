from typing import Dict, Any
import asyncio
from datetime import datetime
from .base_agent import BaseAgent, SharedState
from ..schemas.data_schema import DataSet, DataPoint, Metadata, DataSource

class WorldBankAgent(BaseAgent):
    def __init__(self):
        super().__init__("WorldBank")
        self.base_url = "https://api.worldbank.org/v2"
        # Extended indicators mapping from wb_indicators.py
        self.indicators_mapping = {
            # Economic indicators
            "gdp": "NY.GDP.MKTP.CD", #'Currency': 'USD'
            "gdp per capita": "NY.GDP.PCAP.CD",
            "gdp growth": "NY.GDP.MKTP.KD.ZG",
            "gni": "NY.GNP.MKTP.CD",
            "gni per capita": "NY.GNP.PCAP.CD",
            
            # Population and Social indicators
            "population": "SP.POP.TOTL",
            "population growth": "SP.POP.GROW",
            "urban population": "SP.URB.TOTL",
            "life expectancy": "SP.DYN.LE00.IN",
            "mortality rate": "SP.DYN.IMRT.IN",
            
            # Education indicators
            "literacy rate": "SE.ADT.LITR.ZS",
            "primary enrollment": "SE.PRM.ENRR",
            "secondary enrollment": "SE.SEC.ENRR",
            
            # Economic and Financial indicators
            "inflation": "FP.CPI.TOTL.ZG",
            "unemployment": "SL.UEM.TOTL.ZS",
            "exports": "NE.EXP.GNFS.CD",
            "imports": "NE.IMP.GNFS.CD",
            "fdi": "BX.KLT.DINV.CD.WD",
        }

    def get_available_indicators(self) -> list[str]:
        """Return list of available indicators"""
        return sorted(self.indicators_mapping.keys())

    async def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from World Bank API
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
        
        url = f"{self.base_url}/country/{country}/indicator/{indicator_code}"
        query_params = {
            "format": "json",
            "per_page": 1000,
            "date": f"{start_year}:{end_year}"
        }

        print(f"World Bank API URL: {url}")

        async def _fetch():
            async with self.session.get(url, params=query_params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"World Bank API error: {error_text}")
                return await response.json()

        return await self.handle_retry(_fetch)

    async def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform World Bank data into unified schema
        """
        try:
            if not raw_data or len(raw_data) < 2:
                raise ValueError("Invalid response from World Bank API")

            metadata_raw = raw_data[0]
            data_points = raw_data[1]

            if not data_points:
                raise ValueError("No data points found in the response")

            # Get indicator details from the first data point
            first_point = data_points[0]
            indicator_details = first_point.get("indicator", {})

            transformed_data_points = []
            for point in data_points:
                if point.get("value") is not None:  # Only include points with values
                    transformed_data_points.append(
                        DataPoint(
                            value=point.get("value"),
                            year=int(point.get("date")),
                            country_code=point.get("countryiso3code", ""),
                            country_name=point.get("country", {}).get("value", ""),
                            additional_info={
                                "decimal": point.get("decimal", 0),
                                "indicator_id": indicator_details.get("id", ""),
                                "indicator_name": indicator_details.get("value", "")
                            }
                        )
                    )

            # Determine the unit of the data based on the first data point
            if transformed_data_points:
                first_value = transformed_data_points[0].value
                unit = self.determine_unit(first_value)
                print(f"Determined unit for World Bank data: {unit}")  # Print the determined unit
                SharedState.set_wb_unit(unit)  # Set the determined unit in SharedState
            else:
                unit = "unknown"

            # Sort data points by year
            transformed_data_points.sort(key=lambda x: x.year)

            dataset = DataSet(
                metadata=Metadata(
                    source=DataSource.WORLD_BANK,
                    indicator_code=indicator_details.get("id", ""),
                    indicator_name=indicator_details.get("value", ""),
                    last_updated=datetime.now(),
                    frequency="yearly",
                    unit=unit  # Store the determined unit
                ),
                data=transformed_data_points
            )

            return dataset.dict()
        except Exception as e:
            self.logger.error(f"Error transforming World Bank data: {str(e)}")
            raise 