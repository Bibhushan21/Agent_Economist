function formatAnalysis(analysis) {
    const lines = analysis.split('\n');
    let formatted = '<h3>Analysis Summary</h3><ul>';
    lines.forEach(line => {
        if (line.trim()) {
            formatted += `<li>${line}</li>`;
        }
    });
    formatted += '</ul>';
    return formatted;
}

function formatData(data) {
    let formatted = '<h3>Data Points</h3><table><tr><th>Year</th><th>Value</th></tr>';
    data.forEach(point => {
        formatted += `<tr><td>${point.year}</td><td>${point.value.toLocaleString()}</td></tr>`;
    });
    formatted += '</table>';
    return formatted;
}

let currentChart = null;

async function renderVisualization(mergedData, chartType = 'line') {
    // Show loading spinner
    document.getElementById('graphSpinner').style.display = 'block';

    try {
        const response = await fetch('/mcp/visualize', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ merged_data: mergedData })
        });
        
        if (!response.ok) {
            throw new Error(`Visualization error: ${response.status}`);
        }
        
        const visualData = await response.json();

        // Hide loading spinner
        document.getElementById('graphSpinner').style.display = 'none';

        const ctx = document.getElementById('visualizationChart').getContext('2d');

        // Destroy the existing chart if it exists
        if (currentChart) {
            currentChart.destroy();
        }

        // Create a new chart
        currentChart = new Chart(ctx, {
            type: chartType,
            data: {
                labels: visualData.years,
                datasets: [{
                    label: 'Value',
                    data: visualData.values,
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 2,
                    fill: false
                }]
            },
            options: {
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Year'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Value'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error rendering visualization:', error);
        document.getElementById('graphSpinner').style.display = 'none';
        document.getElementById('visualizationChart').parentElement.innerHTML += 
            `<div class="error-message">Error loading visualization: ${error.message}</div>`;
    }
}

// Add event listener for graph type button clicks
const graphTypeButtons = document.getElementById('graphTypeButtons');
graphTypeButtons.addEventListener('click', async function(event) {
    if (event.target.tagName === 'BUTTON') {
        const selectedType = event.target.getAttribute('data-type');
        const mergedData = JSON.parse(document.getElementById('rawData').dataset.mergedData);
        await renderVisualization(mergedData, selectedType);
    }
});

// Progressive loading implementation
async function fetchDataProgressively(query) {
    // Clear previous results
    document.getElementById('rawData').innerHTML = '';
    document.getElementById('aiAnalysis').innerHTML = '';
    
    // Show loading indicators
    document.getElementById('rawDataSpinner').style.display = 'block';
    document.getElementById('aiAnalysisSpinner').style.display = 'block';
    document.getElementById('graphSpinner').style.display = 'block';
    
    try {
        // Step 1: Fetch raw data first
        const rawDataResponse = await fetch('/mcp/fetch', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query, fetch_only: true }) // New parameter to indicate we only want raw data
        });
        
        if (!rawDataResponse.ok) {
            throw new Error(`Server error: ${rawDataResponse.status}`);
        }
        
        const rawDataResult = await rawDataResponse.json();
        
        // Hide raw data spinner
        document.getElementById('rawDataSpinner').style.display = 'none';
        
        // Format and display raw data immediately
        if (rawDataResult.datasets && rawDataResult.datasets.length > 0 && rawDataResult.datasets[0].data) {
            const dataHtml = formatData(rawDataResult.datasets[0].data);
            document.getElementById('rawData').innerHTML = dataHtml;
            document.getElementById('rawData').dataset.mergedData = JSON.stringify(rawDataResult.datasets[0]);
            
            // Start visualization process
            renderVisualization(rawDataResult.datasets[0], 'line');
        } else {
            document.getElementById('rawData').innerHTML = 
                '<div class="warning-message">No data available for this query.</div>';
        }
        
        // Step 2: Fetch analysis in parallel
        const analysisPromise = fetch('/mcp/analyze', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                country: rawDataResult.query_params.country,
                indicator: rawDataResult.query_params.indicator,
                dataset: rawDataResult.datasets[0]
            })
        });
        
        // Wait for analysis to complete
        const analysisResponse = await analysisPromise;
        
        if (!analysisResponse.ok) {
            throw new Error(`Analysis error: ${analysisResponse.status}`);
        }
        
        const analysisResult = await analysisResponse.json();
        
        // Hide analysis spinner
        document.getElementById('aiAnalysisSpinner').style.display = 'none';
        
        // Format and display analysis
        if (analysisResult.analysis) {
            const analysisHtml = formatAnalysis(analysisResult.analysis);
            document.getElementById('aiAnalysis').innerHTML = analysisHtml;
        } else {
            document.getElementById('aiAnalysis').innerHTML = 
                '<div class="warning-message">No analysis available for this data.</div>';
        }
        
    } catch (error) {
        console.error('Error in progressive loading:', error);
        document.getElementById('rawDataSpinner').style.display = 'none';
        document.getElementById('aiAnalysisSpinner').style.display = 'none';
        document.getElementById('graphSpinner').style.display = 'none';
        
        document.getElementById('rawData').innerHTML = 
            `<div class="error-message">Error: ${error.message}</div>`;
        document.getElementById('aiAnalysis').innerHTML = 
            '<div class="error-message">Analysis could not be performed due to an error.</div>';
    }
}

// Update the form submission to use progressive loading
const queryForm = document.getElementById('query-form');
queryForm.addEventListener('submit', async function(event) {
    event.preventDefault();
    const query = document.getElementById('query').value;
    console.log('Submitting query:', query);
    
    // Set a timeout to show a message if the request takes too long
    const timeoutId = setTimeout(() => {
        document.getElementById('rawData').innerHTML = 
            '<div class="info-message">Request is taking longer than usual. Please wait...</div>';
    }, 5000);
    
    // Use the progressive loading function
    await fetchDataProgressively(query);
    
    clearTimeout(timeoutId);
});