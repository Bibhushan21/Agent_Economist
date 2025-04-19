IMF = https://www.imf.org/external/datamapper/api/v1/{indicators_id}/{country}?periods=2000,2025


 curl -X POST http://localhost:5000/mcp/fetch -H "Content-Type: application/json" -d '{"query": "Give me GDP per capita of India from 2000 to 2025"}'
     curl -X POST http://localhost:5000/mcp/analyze -H "Content-Type: application/json" -d '{"country": "India", "indicator": "GDP", "dataset": {}}'

         