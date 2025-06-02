"""FastAPI endpoints for managing reports."""

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime

# Import dependencies
from app.core.database import get_db_session, get_dw_session
from app.models.reports import (
    SavedReport, ReportRepository,
    ReportConfigRequest, ReportConfigResponse, ReportDropdownData
)
from app.models.calculations import SavedCalculation, CalculationRepository
from app.services.calculation_builder import CalculationManager, AggregationLevel

router = APIRouter()

# Helper functions
def report_to_response_model(report: SavedReport) -> ReportConfigResponse:
    """Convert SavedReport to response model."""
    return ReportConfigResponse(
        id=report.id,
        name=report.name,
        description=report.description,
        aggregation_level=report.aggregation_level,
        calculation_ids=report.calculation_ids,
        cycle_filter=report.cycle_filter,
        additional_filters=report.additional_filters,
        created_at=report.created_at,
        updated_at=report.updated_at
    )

@router.get("/reports/dropdown-data", response_model=ReportDropdownData)
async def get_report_dropdown_data(db: Session = Depends(get_db_session)):
    """Get all dropdown data needed for the report builder UI."""
    
    calc_repo = CalculationRepository(db)
    report_repo = ReportRepository(db)
    
    # Get available calculations
    saved_calcs = calc_repo.get_all_calculations()
    available_calculations = [
        {
            "id": calc.id,
            "name": calc.name,
            "description": calc.description,
            "calculation_type": calc.calculation_type,
            "target_field": calc.target_field,
            "aggregation_level": calc.aggregation_level
        }
        for calc in saved_calcs
    ]
    
    # Get aggregation levels
    aggregation_levels = [
        {"value": "deal", "label": "Deal Level"},
        {"value": "tranche", "label": "Tranche Level"}
    ]
    
    # Get saved reports
    saved_reports_data = report_repo.get_all_reports()
    saved_reports = [report_to_response_model(report) for report in saved_reports_data]
    
    return ReportDropdownData(
        available_calculations=available_calculations,
        aggregation_levels=aggregation_levels,
        saved_reports=saved_reports
    )

@router.post("/reports", response_model=ReportConfigResponse)
async def create_report(
    request: ReportConfigRequest,
    db: Session = Depends(get_db_session)
):
    """Create a new saved report."""
    
    try:
        # Validate that calculation IDs exist
        calc_repo = CalculationRepository(db)
        for calc_id in request.calculation_ids:
            calc = calc_repo.get_calculation(calc_id)
            if not calc:
                raise HTTPException(status_code=400, detail=f"Calculation ID {calc_id} not found")
        
        # Save to database
        repo = ReportRepository(db)
        saved_report = repo.save_report(
            name=request.name,
            description=request.description,
            aggregation_level=request.aggregation_level,
            calculation_ids=request.calculation_ids,
            cycle_filter=request.cycle_filter,
            additional_filters=request.additional_filters
        )
        
        return report_to_response_model(saved_report)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid report configuration: {str(e)}")

@router.get("/reports", response_model=List[ReportConfigResponse])
async def get_reports(
    search: Optional[str] = Query(None, description="Search term"),
    db: Session = Depends(get_db_session)
):
    """Get all saved reports."""
    
    repo = ReportRepository(db)
    
    if search:
        reports = repo.search_reports(search)
    else:
        reports = repo.get_all_reports()
    
    return [report_to_response_model(report) for report in reports]

@router.get("/reports/{report_id}", response_model=ReportConfigResponse)
async def get_report(
    report_id: int,
    db: Session = Depends(get_db_session)
):
    """Get a specific report by ID."""
    
    repo = ReportRepository(db)
    report = repo.get_report(report_id)
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    return report_to_response_model(report)

@router.put("/reports/{report_id}", response_model=ReportConfigResponse)
async def update_report(
    report_id: int,
    request: ReportConfigRequest,
    db: Session = Depends(get_db_session)
):
    """Update an existing report."""
    
    repo = ReportRepository(db)
    report = repo.get_report(report_id)
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    try:
        # Validate that calculation IDs exist
        calc_repo = CalculationRepository(db)
        for calc_id in request.calculation_ids:
            calc = calc_repo.get_calculation(calc_id)
            if not calc:
                raise HTTPException(status_code=400, detail=f"Calculation ID {calc_id} not found")
        
        # Update the report
        updated_report = repo.update_report(
            report_id,
            name=request.name,
            description=request.description,
            aggregation_level=request.aggregation_level,
            calculation_ids=request.calculation_ids,
            cycle_filter=request.cycle_filter,
            additional_filters=request.additional_filters
        )
        
        return report_to_response_model(updated_report)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid report configuration: {str(e)}")

@router.delete("/reports/{report_id}")
async def delete_report(
    report_id: int,
    db: Session = Depends(get_db_session)
):
    """Delete a report (soft delete)."""
    
    repo = ReportRepository(db)
    report = repo.get_report(report_id)
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    repo.delete_report(report_id)
    return {"message": "Report deleted successfully"}

@router.get("/reports/{report_id}/execute")
async def execute_report(
    report_id: int,
    override_cycle_filter: Optional[int] = Query(None, description="Override cycle filter for this execution"),
    limit: int = Query(100, description="Limit results for execution"),
    db: Session = Depends(get_db_session),
    dw_db: Session = Depends(get_dw_session)
):
    """Execute a saved report and return results."""
    
    repo = ReportRepository(db)
    calc_repo = CalculationRepository(db)
    report = repo.get_report(report_id)
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    try:
        # Get the calculations used in this report
        calculation_configs = []
        calculation_details = []
        
        for calc_id in report.calculation_ids:
            calc = calc_repo.get_calculation(calc_id)
            if calc:
                calc_config = calc.to_calculation_config()
                
                # Override cycle filter if provided
                if override_cycle_filter:
                    calc_config.cycle_filter = override_cycle_filter
                elif report.cycle_filter:
                    calc_config.cycle_filter = report.cycle_filter
                
                calculation_configs.append(calc_config)
                calculation_details.append({
                    "id": calc.id,
                    "name": calc.name,
                    "type": calc.calculation_type,
                    "field": calc.target_field
                })
        
        if not calculation_configs:
            raise HTTPException(status_code=400, detail="No valid calculations found for this report")
        
        # Validate all calculations are at the same aggregation level
        report_agg_level = AggregationLevel(report.aggregation_level)
        for config in calculation_configs:
            if config.aggregation_level != report_agg_level:
                raise HTTPException(
                    status_code=400, 
                    detail=f"All calculations must be at {report.aggregation_level} level"
                )
        
        # Build and execute the report query
        manager = CalculationManager(dw_db)
        
        if report_agg_level == AggregationLevel.DEAL:
            enhanced_query = manager.create_enhanced_query("deal", calculation_configs)
        else:
            enhanced_query = manager.create_enhanced_query("tranche", calculation_configs)
        
        # Execute with limit
        results = enhanced_query.limit(limit).all()
        
        # Convert results to serializable format
        result_data = []
        column_names = [desc['name'] for desc in enhanced_query.column_descriptions]
        
        for row in results:
            row_dict = {}
            if hasattr(row, '_fields'):
                for column in row._fields:
                    value = getattr(row, column)
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    row_dict[column] = value
            else:
                for i, col_name in enumerate(column_names):
                    try:
                        value = row[i] if hasattr(row, '__getitem__') else getattr(row, col_name, None)
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        row_dict[col_name] = value
                    except (IndexError, AttributeError):
                        row_dict[col_name] = None
            result_data.append(row_dict)
        
        return {
            "report": report_to_response_model(report),
            "calculations_used": calculation_details,
            "results": result_data,
            "result_count": len(result_data),
            "column_names": column_names,
            "cycle_filter_used": override_cycle_filter or report.cycle_filter,
            "aggregation_level": report.aggregation_level
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing report: {str(e)}")