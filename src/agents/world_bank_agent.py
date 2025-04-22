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

            "BCA": "Current account balance",
            "BCA_NGDPD": "Current account balance",
            "BF": "Financial account balance",
            "BFD": "Direct investment, net",
            "BFF": "Financial derivatives, net",
            "BFO": "Other investment, net",
            "BFP": "Portfolio investment, net",
            "BFRA": "Change in reserves",
            "BM": "Imports of goods and services",
            "BX": "Exports of goods and services",
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
            "GGR": "General government revenue",
            "GGR_NGDP": "General government revenue",
            "GGSB": "General government structural balance",
            "GGSB_NPGDP": "General government structural balance",
            "GGX": "General government total expenditure",
            "GGX_NGDP": "General government total expenditure",
            "GGXCNL": "General government net lending/borrowing",
            "GGXCNL_NGDP": "General government net lending/borrowing",
            "GGXONLB": "General government primary net lending/borrowing",
            "GGXONLB_NGDP": "General government primary net lending/borrowing",
            "GGXWDG": "General government gross debt",
            "GGXWDG_NGDP": "General government gross debt",
            "GGXWDN": "General government net debt",
            "GGXWDN_NGDP": "General government net debt",
            "LE": "Employment",
            "LP": "Population",
            "LUR": "Unemployment rate",
            "NGAP_NPGDP": "Output gap",
            "NGDP_D": "Gross domestic product, deflator",
            "NGDP_FY": "Gross domestic product corresponding to fiscal year, current prices",
            "NGDP_R": "Gross domestic product, constant prices",
            "NGDP_RPCH": "Gross domestic product, constant prices",
            "NGDP_RPCHMK": "Gross domestic product, constant prices",
            "NGDPD": "Gross domestic product, current prices",
            "NGDPDPC": "Gross domestic product per capita, current prices",
            "NGDPPC": "Gross domestic product per capita, current prices",
            "NGDPRPC": "Gross domestic product per capita, constant prices",
            "NGDPRPPPPC": "Gross domestic product per capita, constant prices",
            "NGSD_NGDP": "Gross national savings",
            "NID_NGDP": "Investment",
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
            "PCPI": "Inflation, average consumer prices",
            "PCPIE": "Inflation, end of period consumer prices",
            "PCPIEPCH": "Inflation, end of period consumer prices",
            "PCPIPCH": "Inflation, average consumer prices",
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