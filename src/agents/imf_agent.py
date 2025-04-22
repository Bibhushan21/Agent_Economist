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
            
                "BF": "Financial account balance",
                "BFD": "Direct investment, net",
                "BFF": "Financial derivatives, net",
                "BFO": "Other investment, net",
                "BFP": "Portfolio investment, net",
                "BFRA": "Change in reserves",
                "D": "External debt, total",
                "D_BX": "External debt, total",
                "D_NGDPD": "External debt, total",
                "DS": "External debt, total debt service",
                "DS_BX": "External debt, total debt service",
                "DS_NGDPD": "External debt, total debt service",
                "DSI": "External debt, total debt service, interest",
                "DSI_BX": "External debt, total debt service, interest",
                "DSI_NGDPD": "External debt, total debt service, interest",
                "DSP": "External debt, total debt service, amortization",
                "DSP_BX": "External debt, total debt service, amortization",
                "DSP_NGDPD": "External debt, total debt service, amortization",
                "FLIBOR3": "Three-month London interbank offered rate (LIBOR)",
                "FLIBOR6": "Six-month London interbank offered rate (LIBOR)",
                "GGSB": "General government structural balance",
                "GGSB_NPGDP": "General government structural balance",
                "GGXWDN": "General government net debt",
                "GGXWDN_NGDP": "General government net debt",
                "NGAP_NPGDP": "Output gap",
                "NGDP_D": "Gross domestic product, deflator",
                "NGDP_FY": "Gross domestic product corresponding to fiscal year, current prices",
                "NGDP_R": "Gross domestic product, constant prices",
                "NGDP_RPCHMK": "Gross domestic product, constant prices",
                "NGDPPC": "Gross domestic product per capita, current prices",
                "NGDPRPPPPC": "Gross domestic product per capita, constant prices",
                "PALLFNFW": "Commodity Price Index includes both Fuel and Non-Fuel Price Indices",
                "PALUM": "Aluminum, 99.5% minimum purity, LME spot price, CIF UK ports, US$ per metric tonne",
                "PBANSOP": "Bananas, Central American and Ecuador, FOB U.S. Ports, US$ per metric tonne",
                "PBARL": "Barley, Canadian no.1 Western Barley, spot price, US$ per metric tonne",
                "PBEEF": "Beef, Australian and New Zealand 85% lean fores, FOB U.S. import price, US cents per pound",
                "PBEVEW": "Commodity Beverage Price Index includes Coffee, Tea, and Cocoa",
                "PCEREW": "Commodity Cereals Price Index includes Wheat, Maize (Corn), Rice, and Barley",
                "PCOALAU": "Coal, Australian thermal coal, 1200- btu/pound, less than 1% sulfur, 14% ash, FOB Newcastle/Port Kembla, US$ per metric tonne",
                "PCOALSA": "Coal, South African export price, US$ per metric tonne",
                "PCOALW": "Commodity Coal Price Index includes Australian and South African Coal",
                "PCOCO": "Cocoa beans, International Cocoa Organization cash price, CIF US and European ports, US$ per metric tonne",
                "PCOFFOTM": "Coffee, Other Mild Arabicas, International Coffee Organization New York cash price, ex-dock New York, US cents per pound",
                "PCOFFROB": "Coffee, Robusta, International Coffee Organization New York cash price, ex-dock New York, US cents per pound",
                "PCOFFW": "Commodity Coffee Price Index includes Other Mild Arabicas and Robusta",
                "PCOPP": "Copper, grade A cathode, LME spot price, CIF European ports, US$ per metric tonne",
                "PCOTTIND": "Cotton, Cotton Outlook `A Index`, Middling 1-3/32 inch staple, CIF Liverpool, US cents per pound",
                "PFANDBW": "Commodity Food and Beverage Price Index includes Food and Beverage Price Indices",
                "PFISH": "Fishmeal, Peru Fish meal/pellets 65% protein, CIF, US$ per metric tonne",
                "PFOODW": "Commodity Food Price Index includes Cereal, Vegetable Oils, Meat, Seafood, Sugar, Bananas, and Oranges Price Indices",
                "PGNUTS": "Groundnuts (peanuts), 40/50 (40 to 50 count per ounce), cif Argentina, US$ per metric tonne",
                "PHARDW": "Commodity Hardwood Price Index includes Hardwood Logs and Hardwood Sawn Price Indices",
                "PHIDE": "Hides, Heavy native steers, over 53 pounds, wholesale dealer`s price, US cents per pound",
                "PINDUW": "Commodity Industrial Inputs Price Index includes Agricultural Raw Materials and Metals Price Indices",
                "PIORECR": "Iron Ore, China import Iron Ore Fines 62% FE spot (CFR Tianjin port) US$ per metric ton",
                "PLAMB": "Lamb, frozen carcass Smithfield London, US cents per pound",
                "PLEAD": "Lead, 99.97% pure, LME spot price, CIF European Ports, US$ per metric tonne",
                "PLOGORE": "Soft Logs, Average Export price from the U.S. for Douglas Fir, US$ per cubic meter",
                "PLOGSK": "Hard Logs, Best quality Malaysian meranti, import price Japan, US$ per cubic meter",
                "PMAIZMT": "Maize (corn), U.S. No.2 Yellow, FOB Gulf of Mexico, U.S. price, US$ per metric tonne",
                "PMEATW": "Commodity Meat Price Index includes Beef, Lamb, Swine (pork), and Poultry Price Indices",
                "PMETAW": "Commodity Metals Price Index includes Copper, Aluminum, Iron Ore, Tin, Nickel, Zinc, Lead, and Uranium Price Indices",
                "PNFUELW": "Commodity Non-Fuel Price Index includes Food and Beverages and Industrial Inputs Price Indices",
                "PNGASEU": "Natural Gas, Russian Natural Gas border price in Germany, US$ per million metric British thermal units of gas",
                "PNGASJP": "Natural Gas, Indonesian Liquified Natural Gas in Japan, US$ per million metric British thermal units of liquid",
                "PNGASUS": "Natural Gas, Natural Gas spot price at the Henry Hub terminal in Louisiana, US$ per million metric British thermal units of gas",
                "PNGASW": "Commodity Natural Gas Price Index includes European, Japanese, and American Natural Gas Price Indices",
                "PNICK": "Nickel, melting grade, LME spot price, CIF European ports, US$ per metric tonne",
                "PNRGW": "Commodity Fuel (energy) Index includes Crude oil (petroleum), Natural Gas, and Coal Price Indices",
                "POILAPSP": "Crude Oil (petroleum), simple average of three spot prices; Dated Brent, West Texas Intermediate, and the Dubai Fateh, US$ per barrel",
                "POILAPSPW": "Crude Oil (petroleum), Price index simple average of three spot prices (APSP); Dated Brent, West Texas Intermediate, and the Dubai Fateh",
                "POILBRE": "Crude Oil (petroleum),  Dated Brent, light blend 38 API, fob U.K., US$ per barrel",
                "POILDUB": "Oil; Dubai, medium, Fateh 32 API, fob Dubai Crude Oil (petroleum), Dubai Fateh Fateh 32 API, US$ per barrel",
                "POILWTI": "Crude Oil (petroleum), West Texas Intermediate 40 API, Midland Texas, US$ per barrel",
                "POLVOIL": "Olive Oil, extra virgin less than 1% free fatty acid, ex-tanker price U.K., US$ per metric tonne",
                "PORANG": "Oranges, miscellaneous oranges French import price, US$ per metric tonne",
                "PPOIL": "Palm oil, Malaysia Palm Oil Futures (first contract forward) 4-5 percent FFA, US$ per metric tonne",
                "PPORK": "Swine (pork), 51-52% lean Hogs, U.S. price, US cents per pound",
                "PPOULT": "Poultry (chicken), Whole bird spot price, Georgia docks, US cents per pound",
                "PPPEX": "Implied PPP conversion rate",
                "PPPGDP": "Gross domestic product based on purchasing-power-parity (PPP) valuation of country GDP",
                "PPPPC": "Gross domestic product based on purchasing-power-parity (PPP) per capita GDP",
                "PPPSH": "Gross domestic product based on purchasing-power-parity (PPP) share of world total",
                "PRAWMW": "Commodity Agricultural Raw Materials Index includes Timber, Cotton, Wool, Rubber, and Hides Price Indices",
                "PRICENPQ": "Rice, 5 percent broken milled white rice, Thailand nominal price quote, US$ per metric tonne",
                "PROIL": "Rapeseed oil, crude, FOB Rotterdam, US$ per metric ton",
                "PRUBB": "Rubber, No.1 Rubber Smoked Sheet, FOB Maylaysian/Singapore, US cents per pound",
                "PSALM": "Fish (salmon), Farm Bred Norwegian Salmon, export price, US$ per kilogram",
                "PSAWMAL": "Hard Sawnwood, Dark Red Meranti, select and better quality, C&F U.K port, US$ per cubic meter",
                "PSAWORE": "Soft Sawnwood, average export price of Douglas Fir, U.S. Price, US$ per cubic meter",
                "PSEAFW": "Commodity Seafood Index includes Fish (salmon) and Shrimp Price Indices",
                "PSHRI": "Shrimp, Frozen shell-on headless, block 16/20 count, Indian origin, C&F Japan, US$ per kilogram",
                "PSMEA": "Soybean Meal, Chicago Soybean Meal Futures (first contract forward) Minimum 48 percent protein, US$ per metric tonne",
                "PSOFTW": "Commodity Softwood Index includes Softwood Sawn and Softwood Logs Price Indices",
                "PSOIL": "Soybean Oil, Chicago Soybean Oil Futures (first contract forward) exchange approved grades, US$ per metric tonne",
                "PSOYB": "Soybeans, U.S. soybeans, Chicago Soybean futures contract (first contract forward) No. 2 yellow and par, US$ per metric tonne",
                "PSUGAEEC": "Sugar, European import price, CIF Europe, US cents per pound",
                "PSUGAISA": "Sugar, Free Market, Coffee Sugar and Cocoa Exchange (CSCE) contract no.11 nearest future position, US cents per pound",
                "PSUGAUSA": "Sugar, U.S. import price, contract no.14 nearest futures position, US cents per pound",
                "PSUGAW": "Commodity Sugar Index includes European, Free market, and U.S. Price Indices",
                "PSUNO": "Sunflower Oil, US export price from Gulf of Mexico, US$ per metric tonne",
                "PTEA": "Tea, Mombasa, Kenya, Auction Price, US cents per kilogram",
                "PTIMBW": "Commodity Timber Index includes Hardwood and Softwood Price Indices",
                "PTIN": "Tin, standard grade, LME spot price, US$ per metric tonne",
                "PURAN": "Uranium, u3o8 restricted price, Nuexco exchange spot, US$ per pound",
                "PVOILW": "Commodity Vegetable Oil Index includes Soybean, Soybean Meal, Soybean Oil, Rapeseed Oil, Palm Oil, Sunflower Oil, Olive Oil, Fishmeal, and Groundnut Price Indices",
                "PWHEAMT": "Wheat, No.1 Hard Red Winter, ordinary protein, FOB Gulf of Mexico, US$ per metric tonne",
                "PWOOLC": "Wool, coarse, 23 micron, Australian Wool Exchange spot quote, US cents per kilogram",
                "PWOOLF": "Wool, fine, 19 micron, Australian Wool Exchange spot quote, US cents per kilogram",
                "PWOOLW": "Commodity Wool Index includes Coarse and Fine Wool Price Indices",
                "PZINC": "Zinc, high grade 98% pure, US$ per metric tonne",
                "TM_RPCH": "Volume of imports of goods and services",
                "TMG_RPCH": "Volume of Imports of goods",
                "TRADEPCH": "Trade volume of goods and services",
                "TTPCH": "Terms of trade of goods and services",
                "TTTPCH": "Terms of trade of goods",
                "TX_RPCH": "Volume of exports of goods and services",
                "TXG_RPCH": "Volume of exports of goods",
                "TXGM_D": "Export price of manufactures",
                "TXGM_DPCH": "Export price of manufactures"
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
    