from typing import Dict, Any
import asyncio
from datetime import datetime
import aiohttp
from .base_agent import BaseAgent, SharedState , conversion_factors
from ..schemas.data_schema import DataSet, DataPoint, Metadata, DataSource

class IMFAgent(BaseAgent):
    def __init__(self):
        super().__init__("IMF")
        self.base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc"
        self.indicators_mapping = {
    
                # National Accounts
                "gdp": "NGDPD",  # Gross Domestic Product, current prices (USD)
                "gdp per capita": "NGDPDPC",  # GDP per capita, current prices (USD)
                "gdp growth": "NGDP_RPCH",  # Real GDP growth (annual % change)
                "gdp constant ppp": "NGDPRPPP",  # GDP, constant prices (PPP)
                "gdp per capita constant": "NGDPRPC",  # GDP per capita, constant prices
                "gross national savings": "NGSD_NGDP",  # Gross national savings (% of GDP)
                "investment": "NID_NGDP",  # Investment (% of GDP)
                "output gap": "NGAP_NGDP",  # Output gap (% of potential GDP)

                # Prices and Inflation
                "inflation average": "PCPIPCH",  # Inflation, average consumer prices (%)
                "inflation end of period": "PCPIEPCH",  # Inflation, end of period prices (%)
                "inflation gdp deflator": "NGDPD_PCH",  # Inflation, GDP deflator (%)

                # Labor Market
                "unemployment": "LUR",  # Unemployment rate (% of labor force)
                "employment": "LE",  # Employment level (millions)

                # External Sector
                "current_account": "BCA",  # Current account balance (USD)
                "current_account_percent_gdp": "BCA_NGDPD",  # Current account balance (% of GDP)
                "exports": "BX",  # Exports of goods and services (USD)
                "imports": "BM",  # Imports of goods and services (USD)
                "terms of trade": "TOT",  # Terms of trade (index)
                "foreign_reserves": "NGDP_FX",  # Foreign exchange reserves (USD)

                # Fiscal Indicators
                "government_debt": "GGXWDG_NGDP",  # Government gross debt (% of GDP)
                "fiscal_balance": "GGXCNL_NGDP",  # Fiscal balance (% of GDP)
                "primary_balance": "GGXONLB_NGDP",  # Primary balance (% of GDP)
                "government_revenue": "GGR_NGDP",  # Government revenue (% of GDP)
                "government_expenditure": "GGX_NGDP",  # Government expenditure (% of GDP)

                # Demographics
                "population": "LP",  # Total population
                "working_age_population": "LPA",  # Working-age population (15-64 years)

                # Exchange Rates
                "exchange_rate_usd": "ENDA",  # Exchange rate (national currency per USD)
                "real_effective_exchange_rate": "REER",  # Real effective exchange rate (index)

                # Monetary Indicators
                "broad_money": "M2",  # Broad money (M2, national currency)
                "interest_rate": "IR",  # Policy interest rate (%)

                # Miscellaneous
                "commodity_price_index": "PMP",  # Commodity price index (index)
                "oil_price": "POIL",  # Oil price (USD per barrel)
                "nonfuel_commodity_price": "PNF",  # Non-fuel commodity price index (index)
            }

        

    def get_available_indicators(self) -> list[str]:
        """Return list of available indicators"""
        return sorted(self.indicators_mapping.keys())

    def count_digits_before_decimal(self, value: float) -> int:
        """
        Count the number of digits before the decimal point in a floating-point number.
        
        Args:
            value: A floating-point number
            
        Returns:
            The number of digits before the decimal point
            
        Examples:
            count_digits_before_decimal(1.23456) -> 1
            count_digits_before_decimal(12.3456) -> 2
            count_digits_before_decimal(123.456) -> 3
            count_digits_before_decimal(1234.56) -> 4
        """
        # Convert to string and split at decimal point
        str_value = str(value)
        parts = str_value.split('.')
        
        # Return the length of the integer part
        return len(parts[0])

    def determine_adjustment_factor(self, value: float, target_unit: str) -> float:
        """
        Determine the adjustment factor based on the number of digits before the decimal point
        and the target unit.
        
        Args:
            value: The value to adjust
            target_unit: The target unit (trillions, billions, millions)
            
        Returns:
            The adjustment factor to apply to the value
        """
        digits_before = self.count_digits_before_decimal(value)
        
        # Handle trillions
        if target_unit == "trillions":
            if digits_before == 1:
                return 1000000000  # 1 trillion
            elif digits_before == 2:
                return 1000000000   # 10 trillion
            elif digits_before == 3:
                return 1000000000    # 100 trillion
            elif digits_before == 4:
                return 1000000000     # 1 quadrillion
            else:
                return 1000000000  # Default case
        
        # Handle billions
        elif target_unit == "billions":
            if digits_before == 1:
                return 1000000000  # 1 billion
            elif digits_before == 2:
                return 1000000000   # 10 billion
            elif digits_before == 3:
                return 1000000000    # 100 billion
            elif digits_before == 4:
                return 1000000000     # 1 trillion
            else:
                return 1  # Default case
        
        # Handle millions
        elif target_unit == "millions":
            if digits_before == 1:
                return 1000000  # 1 million
            elif digits_before == 2:
                return 1000000   # 10 million
            elif digits_before == 3:
                return 1000000    # 100 million
            elif digits_before == 4:
                return 1000000     # 1 billion
            else:
                return 1  # Default case
        
        # Default case
        return 1

    async def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from IMF API
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

        # IMF specific endpoint construction
        years = ','.join(str(year) for year in range(int(start_year), int(end_year) + 1))
        url = f"https://www.imf.org/external/datamapper/api/v1/{indicator_code}/{country}?periods={years}"

        print(f"IMF API URL: {url}")

        self.logger.info(f"Fetching data from IMF API with URL: {url}")

        async def _fetch():
            async with self.session.get(url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"IMF API error: {error_text}")
                return await response.json()

        return await self.handle_retry(_fetch)

    async def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform IMF data into unified schema
        """
        try:
            # Extract data series from IMF response
            values = raw_data.get("values", {})
            if not values:
                raise ValueError("No data found in IMF response")

            transformed_data_points = []
            for indicator_code, countries in values.items():
                for country_code, data in countries.items():
                    for year, value in data.items():
                        # Count digits before decimal for this value
                        digits_before_decimal = self.count_digits_before_decimal(value)
                        
                        # Determine the target unit based on World Bank data
                        target_unit = "trillions"  # Default to trillions
                        wb_unit = SharedState.get_wb_unit()
                        if wb_unit != 'unknown':
                            target_unit = wb_unit
                        
                        # Determine the adjustment factor based on the target unit
                        adjustment_factor = self.determine_adjustment_factor(value, target_unit)
                        
                        # Apply the adjustment factor to the value
                        adjusted_value = float(value) * adjustment_factor
                        
                        transformed_data_points.append(
                            DataPoint(
                                value=adjusted_value,
                                year=int(year),
                                country_code=country_code,
                                country_name="",  # Country name not provided in this structure
                                additional_info={
                                    "indicator_id": indicator_code,
                                    "digits_before_decimal": digits_before_decimal,
                                    "original_value": float(value),
                                    "adjustment_factor": adjustment_factor,
                                    "target_unit": target_unit
                                }
                            )
                        )

            # Access the units from SharedState
            un_unit = SharedState.get_un_unit()
            wb_unit = SharedState.get_wb_unit()

            print(f"UN Unit: {un_unit}, WB Unit: {wb_unit}")  # Debugging output

            # Use the same unit as World Bank data when available, otherwise default to trillions
            target_unit = 'trillions'  # Default to trillions
            if wb_unit != 'unknown':
                target_unit = wb_unit
                print(f"Using World Bank unit for IMF data: {target_unit}")  # Debugging output
            else:
                print(f"Using default unit for IMF data: {target_unit}")  # Debugging output

            # Sort data points by year
            transformed_data_points.sort(key=lambda x: x.year)

            dataset = DataSet(
                metadata=Metadata(
                    source=DataSource.IMF,
                    indicator_code=indicator_code,
                    indicator_name="",  # Indicator name not provided in this structure
                    last_updated=datetime.now(),
                    frequency="yearly",  # Assuming yearly frequency
                    unit=target_unit  # Store the target unit
                ),
                data=transformed_data_points
            )

            return dataset.dict()
        except Exception as e:
            self.logger.error(f"Error transforming IMF data: {str(e)}")
            raise 
    