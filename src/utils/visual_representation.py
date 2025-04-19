from typing import Dict, Any, List

def prepare_visual_data(merged_data: Dict[str, Any]) -> Dict[str, List]:
    """
    Prepare data for visual representation.
    """
    years = []
    values = []

    for data_point in merged_data.get("data", []):
        years.append(data_point["year"])
        values.append(data_point["value"])

    return {"years": years, "values": values} 