"""FastAPI endpoints for managing custom calculations."""

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import json
from datetime import datetime

# Import your models and dependencies
from app.core.database import get_db_session, get_dw_session
from app.models.calculations import (
    SavedCalculation, CalculationRepository,
    CalculationConfigRequest, CalculationConfigResponse, DropdownOption, DropdownData
)
from app.models.dwh_models import Deal, Tranche, TrancheBal
from app.services.calculation_builder import (
    DynamicSubqueryBuilder, CalculationManager, CalculationConfig,
    CalculationType, AggregationLevel
)

router = APIRouter()

# Helper functions
def get_calculation_type_description(calc_type: CalculationType) -> str:
    """Get description for calculation type."""
    descriptions = {
        CalculationType.SUM: "Sum all values",
        CalculationType.AVERAGE: "Calculate average", 
        CalculationType.WEIGHTED_AVERAGE: "Calculate weighted average",
        CalculationType.COUNT: "Count records",
        CalculationType.MIN: "Find minimum value",
        CalculationType.MAX: "Find maximum value",
        CalculationType.RATIO: "Calculate ratio between two fields",
        CalculationType.PERCENTAGE: "Calculate percentage between two fields"
    }
    return descriptions.get(calc_type, "")

def calc_to_response_model(calc: SavedCalculation) -> CalculationConfigResponse:
    """Convert SavedCalculation to response model."""
    return CalculationConfigResponse(
        id=calc.id,
        name=calc.name,
        description=calc.description,
        calculation_type=calc.calculation_type,
        target_field=calc.target_field,
        aggregation_level=calc.aggregation_level,
        weight_field=calc.weight_field,
        denominator_field=calc.denominator_field,
        cycle_filter=calc.cycle_filter,
        filters=calc.filters,
        created_at=calc.created_at,
        updated_at=calc.updated_at
    )

def request_to_calculation_config(request: CalculationConfigRequest) -> CalculationConfig:
    """Convert API request to CalculationConfig."""
    return CalculationConfig(
        name=request.name,
        calculation_type=CalculationType(request.calculation_type),
        target_field=request.target_field,
        aggregation_level=AggregationLevel(request.aggregation_level),
        weight_field=request.weight_field,
        denominator_field=request.denominator_field,
        filters=request.filters,
        cycle_filter=request.cycle_filter
    )

@router.get("/calculations/dropdown-data", response_model=DropdownData)
async def get_dropdown_data(db: Session = Depends(get_db_session)):
    """Get all dropdown data needed for the calculation builder UI."""
    
    repo = CalculationRepository(db)
    
    # Get calculation types
    calculation_types = [
        DropdownOption(
            value=calc_type.value,
            label=calc_type.value.replace('_', ' ').title(),
            description=get_calculation_type_description(calc_type)
        )
        for calc_type in CalculationType
    ]
    
    # Get target fields
    target_fields = [
        DropdownOption(value="ending_balance", label="Ending Balance", description="Tranche ending balance amount"),
        DropdownOption(value="principal_release", label="Principal Release", description="Principal release/loss amount"),
        DropdownOption(value="pass_through_rate", label="Pass Through Rate", description="Interest pass-through rate"),
        DropdownOption(value="accrual_days", label="Accrual Days", description="Number of accrual days"),
        DropdownOption(value="interest_distribution", label="Interest Distribution", description="Interest distribution amount"),
        DropdownOption(value="principal_distribution", label="Principal Distribution", description="Principal distribution amount"),
        DropdownOption(value="interest_accrual", label="Interest Accrual", description="Interest accrual amount"),
        DropdownOption(value="interest_shortfall", label="Interest Shortfall", description="Interest shortfall amount"),
    ]
    
    # Get aggregation levels
    aggregation_levels = [
        DropdownOption(value="deal", label="Deal Level", description="Aggregate across all tranches in a deal"),
        DropdownOption(value="tranche", label="Tranche Level", description="Individual tranche calculations"),
    ]
    
    # Get saved calculations
    saved_calcs = repo.get_all_calculations()
    saved_calculations = [calc_to_response_model(calc) for calc in saved_calcs]
    
    return DropdownData(
        calculation_types=calculation_types,
        target_fields=target_fields,
        aggregation_levels=aggregation_levels,
        saved_calculations=saved_calculations
    )

@router.post("/calculations", response_model=CalculationConfigResponse)
async def create_calculation(
    request: CalculationConfigRequest,
    db: Session = Depends(get_db_session)
):
    """Create a new saved calculation."""
    
    try:
        # Validate the calculation configuration
        calc_config = request_to_calculation_config(request)
        
        # Save to database
        repo = CalculationRepository(db)
        saved_calc = repo.save_calculation(
            config=calc_config,
            name=request.name,
            description=request.description
        )
        
        return calc_to_response_model(saved_calc)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid calculation configuration: {str(e)}")

@router.get("/calculations", response_model=List[CalculationConfigResponse])
async def get_calculations(
    search: Optional[str] = Query(None, description="Search term"),
    db: Session = Depends(get_db_session)
):
    """Get all saved calculations."""
    
    repo = CalculationRepository(db)
    
    if search:
        calculations = repo.search_calculations(search)
    else:
        calculations = repo.get_all_calculations()
    
    return [calc_to_response_model(calc) for calc in calculations]

@router.get("/calculations/{calc_id}", response_model=CalculationConfigResponse)
async def get_calculation(
    calc_id: int,
    db: Session = Depends(get_db_session)
):
    """Get a specific calculation by ID."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    return calc_to_response_model(calculation)

@router.put("/calculations/{calc_id}", response_model=CalculationConfigResponse)
async def update_calculation(
    calc_id: int,
    request: CalculationConfigRequest,
    db: Session = Depends(get_db_session)
):
    """Update an existing calculation."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    try:
        # Update the calculation
        updated_calc = repo.update_calculation(
            calc_id,
            name=request.name,
            description=request.description,
            calculation_type=request.calculation_type,
            target_field=request.target_field,
            aggregation_level=request.aggregation_level,
            weight_field=request.weight_field,
            denominator_field=request.denominator_field,
            cycle_filter=request.cycle_filter,
            filters=request.filters
        )
        
        return calc_to_response_model(updated_calc)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid calculation configuration: {str(e)}")

@router.delete("/calculations/{calc_id}")
async def delete_calculation(
    calc_id: int,
    db: Session = Depends(get_db_session)
):
    """Delete a calculation (soft delete)."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    repo.delete_calculation(calc_id)
    return {"message": "Calculation deleted successfully"}

@router.post("/calculations/preview-sql")
async def preview_calculation_sql(
    request: CalculationConfigRequest,
    dw_db: Session = Depends(get_dw_session)
):
    """Generate SQL preview for a calculation configuration."""
    
    try:
        # Convert request to CalculationConfig
        calc_config = request_to_calculation_config(request)
        
        # Build the calculation using data warehouse session
        builder = DynamicSubqueryBuilder(dw_db)
        manager = CalculationManager(dw_db)
        
        # Generate the subquery
        subquery = builder.build_calculation_subquery(calc_config)
        
        # Generate the full query
        if calc_config.aggregation_level == AggregationLevel.DEAL:
            base_query = manager.create_enhanced_query("deal", [calc_config])
        else:
            base_query = manager.create_enhanced_query("tranche", [calc_config])
        
        # Compile SQL with literal binds for better readability
        sql_str = str(base_query.statement.compile(
            dialect=dw_db.bind.dialect,
            compile_kwargs={"literal_binds": True}
        ))
        
        return {
            "sql_preview": sql_str,
            "calculation_name": calc_config.name,
            "calculation_type": calc_config.calculation_type.value,
            "aggregation_level": calc_config.aggregation_level.value
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating SQL preview: {str(e)}")

@router.get("/calculations/{calc_id}/test")
async def test_calculation(
    calc_id: int,
    cycle_filter: Optional[int] = Query(None, description="Cycle to test with"),
    limit: int = Query(10, description="Limit results for testing"),
    db: Session = Depends(get_db_session),
    dw_db: Session = Depends(get_dw_session)
):
    """Test a calculation and return sample results."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    try:
        # Convert to CalculationConfig
        calc_config = calculation.to_calculation_config()
        
        # Override cycle filter if provided
        if cycle_filter:
            calc_config.cycle_filter = cycle_filter
        
        # Build and execute the calculation using data warehouse session
        manager = CalculationManager(dw_db)
        
        if calc_config.aggregation_level == AggregationLevel.DEAL:
            base_query = manager.create_enhanced_query("deal", [calc_config])
        else:
            base_query = manager.create_enhanced_query("tranche", [calc_config])
        
        # Execute with limit
        results = base_query.limit(limit).all()
        
        # Convert results to serializable format
        result_data = []
        for row in results:
            row_dict = {}
            # Handle SQLAlchemy row objects
            if hasattr(row, '_fields'):
                for column in row._fields:
                    value = getattr(row, column)
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    row_dict[column] = value
            else:
                # Handle simple objects
                for i, desc in enumerate(base_query.column_descriptions):
                    col_name = desc['name']
                    try:
                        value = row[i] if hasattr(row, '__getitem__') else getattr(row, col_name, None)
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        row_dict[col_name] = value
                    except (IndexError, AttributeError):
                        row_dict[col_name] = None
            result_data.append(row_dict)
        
        return {
            "calculation": calc_to_response_model(calculation),
            "sample_results": result_data,
            "result_count": len(result_data),
            "cycle_filter_used": calc_config.cycle_filter
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error testing calculation: {str(e)}")