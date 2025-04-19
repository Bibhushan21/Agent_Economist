from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
from enum import Enum

class DataSource(str, Enum):
    WORLD_BANK = "world_bank"
    IMF = "imf"
    OECD = "oecd"
    UN = "un"

class Metadata(BaseModel):
    source: DataSource
    indicator_code: str
    indicator_name: str
    last_updated: datetime
    frequency: str
    unit: str

class DataPoint(BaseModel):
    value: Union[float, int, None]
    year: int
    country_code: str
    country_name: str
    additional_info: Dict[str, Any] = Field(default_factory=dict)

class DataSet(BaseModel):
    metadata: Metadata
    data: List[DataPoint]
    error_log: List[str] = Field(default_factory=list)
    warning_log: List[str] = Field(default_factory=list)

class AggregatedDataResponse(BaseModel):
    """
    Final response format containing data from multiple sources
    """
    query_params: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.now)
    datasets: List[DataSet]
    status: str
    error_summary: Optional[Dict[str, List[str]]] = None
    analyses: Optional[Dict[str, str]] = None  # Analysis results for each data source

    class Config:
        arbitrary_types_allowed = True 