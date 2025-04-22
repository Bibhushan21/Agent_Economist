import asyncio
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from src.agents.master_agent import MasterAgent
from mistralai.client import MistralClient
import re

# Load environment variables at the start
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class QueryParser:
    def __init__(self):
        # Initialize MistralAI client
        mistral_api_key = os.getenv('MISTRAL_API_KEY')  # Load API key from environment variable
        
        # Validate API key format
        if not isinstance(mistral_api_key, str) or len(mistral_api_key) < 32:
            raise ValueError("Invalid Mistral API key format")
        
        self.client = MistralClient(api_key=mistral_api_key)
        
        # Define supported indicators for each source with their unique IDs
        self.indicator_ids = {
            "world_bank": {
                "gdp": "NY.GDP.MKTP.CD",
                "gdp per capita": "NY.GDP.PCAP.CD",
                "population": "SP.POP.TOTL",
                "literacy rate": "SE.ADT.LITR.ZS",
                "gdp growth": "NY.GDP.MKTP.KD.ZG",
                "unemployment": "SL.UEM.TOTL.ZS",
                "inflation": "FP.CPI.TOTL.ZG",
                "life expectancy": "SP.DYN.LE00.IN",
                "poverty": "SI.POV.DDAY"
            },
            "imf": {
                "current_account": "BCA",
                "exports": "TXG_FOB_USD",
                "fiscal_balance": "GGB",
                "foreign_reserves": "RESERVES",
                "gdp": "NGDP",
                "gdp growth": "NGDP_RPCH",
                "government_debt": "GGXWDG_NGDP",
                "imports": "TMG_CIF_USD",
                "inflation": "PCPI",
                "unemployment": "LUR"
            },
            "oecd": {
                "gdp": "GDP",
                "gdp_per_capita": "GDP_CAP",
                "gdp growth": "GDP_GROWTH",
                "government_debt": "GOV_DEBT",
                "household_income": "HH_INC",
                "inflation": "CPI",
                "productivity": "PROD",
                "r_and_d": "RND",
                "trade_balance": "TRADE_BAL",
                "unemployment": "UNEMP"
            },
            "un": {
                "access_electricity": "ELEC_ACCESS",
                "child_mortality": "CHILD_MORT",
                "education_index": "EDU_INDEX",
                "gender_inequality": "GII",
                "human_development": "HDI",
                "internet_users": "INT_USERS",
                "life_expectancy": "LIFE_EXP",
                "maternal_mortality": "MAT_MORT",
                "population": "POP",
                "gdp growth": "GDP_GROWTH_UN"
            }
        }
        
        # Comprehensive ISO 3166-1 alpha-3 country codes mapping
        self.country_codes = {
            "afghanistan": "AFG", "albania": "ALB", "algeria": "DZA", "andorra": "AND",
            "angola": "AGO", "argentina": "ARG", "armenia": "ARM", "australia": "AUS",
            "austria": "AUT", "azerbaijan": "AZE", "bahamas": "BHS", "bahrain": "BHR",
            "bangladesh": "BGD", "barbados": "BRB", "belarus": "BLR", "belgium": "BEL",
            "belize": "BLZ", "benin": "BEN", "bhutan": "BTN", "bolivia": "BOL",
            "bosnia": "BIH", "botswana": "BWA", "brazil": "BRA", "brunei": "BRN",
            "bulgaria": "BGR", "burkina faso": "BFA", "burundi": "BDI", "cambodia": "KHM",
            "cameroon": "CMR", "canada": "CAN", "chad": "TCD", "chile": "CHL",
            "china": "CHN", "colombia": "COL", "congo": "COG", "costa rica": "CRI",
            "croatia": "HRV", "cuba": "CUB", "cyprus": "CYP", "czech republic": "CZE",
            "denmark": "DNK", "djibouti": "DJI", "dominica": "DMA", "dr congo": "COD",
            "ecuador": "ECU", "egypt": "EGY", "el salvador": "SLV", "estonia": "EST",
            "ethiopia": "ETH", "fiji": "FJI", "finland": "FIN", "france": "FRA",
            "gabon": "GAB", "gambia": "GMB", "georgia": "GEO", "germany": "DEU",
            "ghana": "GHA", "greece": "GRC", "guatemala": "GTM", "guinea": "GIN",
            "guyana": "GUY", "haiti": "HTI", "honduras": "HND", "hungary": "HUN",
            "iceland": "ISL", "india": "IND", "indonesia": "IDN", "iran": "IRN",
            "iraq": "IRQ", "ireland": "IRL", "israel": "ISR", "italy": "ITA",
            "jamaica": "JAM", "japan": "JPN", "jordan": "JOR", "kazakhstan": "KAZ",
            "kenya": "KEN", "kuwait": "KWT", "kyrgyzstan": "KGZ", "laos": "LAO",
            "latvia": "LVA", "lebanon": "LBN", "lesotho": "LSO", "liberia": "LBR",
            "libya": "LBY", "liechtenstein": "LIE", "lithuania": "LTU", "luxembourg": "LUX",
            "madagascar": "MDG", "malawi": "MWI", "malaysia": "MYS", "maldives": "MDV",
            "mali": "MLI", "malta": "MLT", "mauritania": "MRT", "mauritius": "MUS",
            "mexico": "MEX", "moldova": "MDA", "monaco": "MCO", "mongolia": "MNG",
            "montenegro": "MNE", "morocco": "MAR", "mozambique": "MOZ", "myanmar": "MMR",
            "namibia": "NAM", "nepal": "NPL", "netherlands": "NLD", "new zealand": "NZL",
            "nicaragua": "NIC", "niger": "NER", "nigeria": "NGA", "north korea": "PRK",
            "norway": "NOR", "oman": "OMN", "pakistan": "PAK", "panama": "PAN",
            "papua new guinea": "PNG", "paraguay": "PRY", "peru": "PER", "philippines": "PHL",
            "poland": "POL", "portugal": "PRT", "qatar": "QAT", "romania": "ROU",
            "russia": "RUS", "rwanda": "RWA", "saudi arabia": "SAU", "senegal": "SEN",
            "serbia": "SRB", "sierra leone": "SLE", "singapore": "SGP", "slovakia": "SVK",
            "slovenia": "SVN", "somalia": "SOM", "south africa": "ZAF", "south korea": "KOR",
            "spain": "ESP", "sri lanka": "LKA", "sudan": "SDN", "sweden": "SWE",
            "switzerland": "CHE", "syria": "SYR", "taiwan": "TWN", "tajikistan": "TJK",
            "tanzania": "TZA", "thailand": "THA", "togo": "TGO", "tunisia": "TUN",
            "turkey": "TUR", "turkmenistan": "TKM", "uganda": "UGA", "ukraine": "UKR",
            "united arab emirates": "ARE", "united kingdom": "GBR", "united states": "USA",
            "usa": "USA", "us": "USA", "uruguay": "URY", "uzbekistan": "UZB",
            "venezuela": "VEN", "vietnam": "VNM", "yemen": "YEM", "zambia": "ZMB",
            "zimbabwe": "ZWE"
        }

        # Common variations and abbreviations
        self.country_variations = {
            "america": "united states",
            "uk": "united kingdom",
            "britain": "united kingdom",
            "great britain": "united kingdom",
            "uae": "united arab emirates",
            "burma": "myanmar",
            "congo dr": "dr congo",
            "democratic republic of congo": "dr congo",
            "republic of korea": "south korea",
            "dprk": "north korea",
            "rok": "south korea",
            "united states of america": "united states",
            "emirates": "united arab emirates"
        }

    def _normalize_country_name(self, country: str) -> str:
        """Normalize country name and handle variations"""
        country = country.lower().strip()
        # Check for variations first
        country = self.country_variations.get(country, country)
        return country

    async def parse_query(self, query: str) -> dict:
        """Parse natural language query using Mistral"""
        try:
            # Create a prompt for Mistral to extract information
            prompt = f"""Extract the following information from this query: "{query}"
1. Indicator type (e.g., GDP, population, literacy rate)
2. Country name (extract the full country name)
3. Start year (if mentioned, default to 2000)
4. End year (if mentioned, default to 2025)

Available indicators are: {', '.join(self.indicator_ids['world_bank'].keys())}

Format the response as JSON:
{{
    "indicator": "extracted_indicator",
    "country": "extracted_country",
    "start_year": year,
    "end_year": year
}}

Note: Be sure to output the complete country name, not abbreviations."""

            # Get response from Mistral
            response = self.client.chat(
                model="mistral-medium",  # Consistently using mistral-medium
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that extracts structured information from queries. Only respond with the requested JSON format."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )

            # Parse the response
            result = eval(response.choices[0].message.content)
            
            # Normalize indicator
            indicator = result["indicator"].lower()
            
            # Collect indicator IDs for all sources that support the indicator
            indicator_ids = {}
            for source, indicators in self.indicator_ids.items():
                if indicator in indicators:
                    indicator_ids[source] = indicators[indicator]
            
            if not indicator_ids:
                available_indicators = ", ".join(self.indicator_ids['world_bank'].keys())
                raise ValueError(f"Unsupported indicator. Available indicators for World Bank are: {available_indicators}")
            
            result["indicator_ids"] = indicator_ids
            
            # Normalize and convert country name to code
            country_name = self._normalize_country_name(result["country"])
            country_code = self.country_codes.get(country_name)
            
            if not country_code:
                similar_countries = [c for c in self.country_codes.keys() 
                                  if country_name in c or c in country_name]
                if similar_countries:
                    suggestion = f"Did you mean: {', '.join(similar_countries)}?"
                    raise ValueError(f"Country code not found for: {result['country']}. {suggestion}")
                raise ValueError(f"Country code not found for: {result['country']}")
            
            result["country"] = country_code
            return result

        except Exception as e:
            raise ValueError(f"Error parsing query: {str(e)}")

async def main():
    load_dotenv()
    
    # Initialize the parser and master agent
    parser = QueryParser()
    master = MasterAgent()
    
    print("\nWelcome to the Economic Data Analysis System!")
    print("You can ask questions like:")
    print("- Give me the GDP per capita of Nepal from 2000 to 2025")
    print("- Show me population data for USA between 2010 and 2022")
    print("- What is the literacy rate in Afghanistan from 2015?")
    print("- Show me the GDP growth in United Kingdom from 2018")
    print("- What is the unemployment rate in South Korea for the last 10 years?\n")
    
    while True:
        try:
            # Get user input
            print("\nEnter your query (or 'quit' to exit):")
            query = input("> ")
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
            
            print("\nAnalyzing your query...")
            # Parse the query using Mistral
            params = await parser.parse_query(query)
            print(f"\nExtracted parameters: {params}")
            
            # print("\nFetching data from multiple sources...")
            # Fetch and analyze data
            result = await master.fetch_with_retry(params)
            
            # Print results
            print("\nResults:")
            print(f"Status: {result.status}")
            
            if result.error_summary:
                print("\nErrors encountered:")
                for source, errors in result.error_summary.items():
                    print(f"{source}: {', '.join(errors)}")
            
            print(f"\nDatasets retrieved: {len(result.datasets)}")
            
            # Print combined analysis if available
            if result.analyses and "combined" in result.analyses:
                print("\nCombined Analysis:")
                print(result.analyses["combined"])
            
            # # Print individual dataset results
            # for dataset in result.datasets:
            #     print(f"\nSource: {dataset.metadata.source}")
            #     print(f"Indicator: {dataset.metadata.indicator_name}")
            #     print(f"Number of data points: {len(dataset.data)}")
                
                # # Print sample data points
                # print("\nSample data points:")
                # for point in sorted(dataset.data, key=lambda x: x.year)[:5]:
                #     print(f"Year: {point.year}, Value: {point.value}")
                
                # Print source-specific analysis
                # if result.analyses and dataset.metadata.source in result.analyses:
                #     print("\nSource-specific Analysis:")
                #     print(result.analyses[dataset.metadata.source])
            
        except Exception as e:
            print(f"\nError: {str(e)}")
            print("Please try again with a different query.")

if __name__ == "__main__":
    asyncio.run(main())
