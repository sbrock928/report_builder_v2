"""API models for report wizard functionality."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from enum import Enum


class ReportScope(str, Enum):
    """Report scope enumeration."""
    DEAL = "DEAL"
    TRANCHE = "TRANCHE"


class FieldSource(str, Enum):
    """Field source enumeration."""
    RAW_FIELD = "raw_field"
    SAVED_CALCULATION = "saved_calculation"


class FilterOperator(str, Enum):
    """Filter operator enumeration."""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


# === REQUEST MODELS ===

class ReportTrancheCreate(BaseModel):
    """Model for creating report tranche associations."""
    dl_nbr: int
    tr_id: str


class ReportDealCreate(BaseModel):
    """Model for creating report deal associations."""
    dl_nbr: int
    selected_tranches: Optional[List[ReportTrancheCreate]] = []


class ReportFieldCreate(BaseModel):
    """Model for creating report field configurations."""
    field_name: str
    display_name: str
    field_type: str  # "text", "number", "date", "percentage"
    field_source: FieldSource = FieldSource.RAW_FIELD
    calculation_id: Optional[int] = None
    is_required: bool = False


class FilterConditionCreate(BaseModel):
    """Model for creating filter conditions."""
    field_name: str
    operator: FilterOperator
    value: Optional[Union[str, int, float, List[Union[str, int, float]]]] = None


class ReportCreate(BaseModel):
    """Model for creating a new report."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    scope: ReportScope
    created_by: str = Field(..., min_length=1, max_length=50)
    selected_deals: List[ReportDealCreate] = []
    selected_fields: List[ReportFieldCreate] = []
    filter_conditions: List[FilterConditionCreate] = []


class ReportUpdate(BaseModel):
    """Model for updating an existing report."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    scope: Optional[ReportScope] = None
    selected_deals: Optional[List[ReportDealCreate]] = None
    selected_fields: Optional[List[ReportFieldCreate]] = None
    filter_conditions: Optional[List[FilterConditionCreate]] = None


class ReportExecuteRequest(BaseModel):
    """Model for executing a report."""
    report_id: int
    cycle_filter: Optional[int] = None
    additional_filters: Optional[List[FilterConditionCreate]] = []


# === RESPONSE MODELS ===

class ReportTrancheResponse(BaseModel):
    """Response model for report tranche."""
    id: int
    dl_nbr: int
    tr_id: str


class ReportDealResponse(BaseModel):
    """Response model for report deal."""
    id: int
    dl_nbr: int
    selected_tranches: List[ReportTrancheResponse] = []


class ReportFieldResponse(BaseModel):
    """Response model for report field."""
    id: int
    field_name: str
    display_name: str
    field_type: str
    field_source: FieldSource
    calculation_id: Optional[int] = None
    is_required: bool


class FilterConditionResponse(BaseModel):
    """Response model for filter condition."""
    id: int
    field_name: str
    operator: FilterOperator
    value: Optional[str] = None


class ReportResponse(BaseModel):
    """Response model for a complete report configuration."""
    id: int
    name: str
    description: Optional[str] = None
    scope: ReportScope
    created_by: str
    created_date: datetime
    updated_date: datetime
    is_active: bool
    selected_deals: List[ReportDealResponse] = []
    selected_fields: List[ReportFieldResponse] = []
    filter_conditions: List[FilterConditionResponse] = []


class ReportSummaryResponse(BaseModel):
    """Response model for report summary (for listing)."""
    id: int
    name: str
    description: Optional[str] = None
    scope: ReportScope
    created_by: str
    created_date: str  # ISO string
    updated_date: str  # ISO string
    deal_count: int
    tranche_count: int
    field_count: int


# === WIZARD DATA MODELS ===

class AvailableField(BaseModel):
    """Available field for selection in wizard."""
    field_name: str
    display_name: str
    description: Optional[str] = None
    field_type: str  # "text", "number", "date", "percentage"
    field_source: FieldSource
    category: str  # "Basic Info", "Financial Data", "Calculations"
    is_default: bool = False
    calculation_id: Optional[int] = None  # If field_source is saved_calculation


class DealInfo(BaseModel):
    """Deal information for wizard."""
    dl_nbr: int
    issr_cde: str
    cdi_file_nme: str
    CDB_cdi_file_nme: Optional[str] = None


class TrancheInfo(BaseModel):
    """Tranche information for wizard."""
    dl_nbr: int
    tr_id: str
    tr_cusip_id: Optional[str] = None


class WizardDataResponse(BaseModel):
    """Response containing all wizard data."""
    available_fields: Dict[str, List[AvailableField]]  # Keyed by scope
    deals: List[DealInfo]
    calculation_types: List[str]
    filter_operators: List[Dict[str, Any]]


# === EXECUTION MODELS ===

class ReportColumn(BaseModel):
    """Report column definition."""
    field: str
    header: str
    type: str  # "text", "number", "date", "percentage"


class ReportExecutionResult(BaseModel):
    """Result of executing a report."""
    report_id: int
    report_name: str
    scope: ReportScope
    columns: List[ReportColumn]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: int
    cycle_filter: Optional[int] = None


class ReportSchemaResponse(BaseModel):
    """Response for report schema (for preview)."""
    report_id: int
    title: str
    columns: List[ReportColumn]
    skeleton_data: List[Dict[str, Any]]  # Sample data showing structure


# === ERROR MODELS ===

class ReportError(BaseModel):
    """Error response model."""
    detail: str
    errors: Optional[List[str]] = None
    report_id: Optional[int] = None