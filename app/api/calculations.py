"""FastAPI endpoints for managing custom calculations."""

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import json
from datetime import datetime

# Import your models and dependencies
from app.core.database import get_db_session, get_dw_session
from app.models.calculations import (
    SavedCalculation, CalculationTemplate, CalculationRepository,
    CalculationConfigRequest, CalculationConfigResponse, DropdownOption, DropdownData
)
from app.models.dwh_models import Deal, Tranche, TrancheBal
from app.services.calculation_builder import (
    DynamicSubqueryBuilder, CalculationManager, CalculationConfig,
    CalculationType, AggregationLevel
)

router = APIRouter()

# Dependency to get current user (implement based on your auth system)
def get_current_user():
    """Get current user from authentication system."""
    # This would integrate with your authentication system
    return "demo_user"

@router.get("/calculations/dropdown-data", response_model=DropdownData)
async def get_dropdown_data(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
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
    
    # Get saved calculations for user
    saved_calcs = repo.get_user_calculations(user_id) + repo.get_public_calculations()
    saved_calculations = [calc_to_response_model(calc) for calc in saved_calcs]
    
    # Get templates
    template_records = repo.get_calculation_templates()
    templates = [template_to_response_model(template) for template in template_records]
    
    return DropdownData(
        calculation_types=calculation_types,
        target_fields=target_fields,
        aggregation_levels=aggregation_levels,
        saved_calculations=saved_calculations,
        templates=templates
    )

@router.post("/calculations", response_model=CalculationConfigResponse)
async def create_calculation(
    request: CalculationConfigRequest,
    user_id: str = Depends(get_current_user),
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
            description=request.description,
            created_by=user_id,
            is_public=request.is_public
        )
        
        return calc_to_response_model(saved_calc)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid calculation configuration: {str(e)}")

@router.get("/calculations", response_model=List[CalculationConfigResponse])
async def get_calculations(
    user_id: str = Depends(get_current_user),
    include_public: bool = Query(True, description="Include public calculations"),
    search: Optional[str] = Query(None, description="Search term"),
    db: Session = Depends(get_db_session)
):
    """Get saved calculations for the current user."""
    
    repo = CalculationRepository(db)
    
    if search:
        calculations = repo.search_calculations(search, user_id if not include_public else None)
    else:
        calculations = repo.get_user_calculations(user_id)
        if include_public:
            calculations += repo.get_public_calculations()
    
    return [calc_to_response_model(calc) for calc in calculations]

@router.get("/calculations/{calc_id}", response_model=CalculationConfigResponse)
async def get_calculation(
    calc_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Get a specific calculation by ID."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    # Check permissions
    if calculation.created_by != user_id and not calculation.is_public:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return calc_to_response_model(calculation)

@router.put("/calculations/{calc_id}", response_model=CalculationConfigResponse)
async def update_calculation(
    calc_id: int,
    request: CalculationConfigRequest,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Update an existing calculation."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    # Check permissions
    if calculation.created_by != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
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
            filters=request.filters,
            is_public=request.is_public
        )
        
        return calc_to_response_model(updated_calc)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid calculation configuration: {str(e)}")

@router.delete("/calculations/{calc_id}")
async def delete_calculation(
    calc_id: int,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    """Delete a calculation (soft delete)."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    # Check permissions
    if calculation.created_by != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    repo.delete_calculation(calc_id)
    return {"message": "Calculation deleted successfully"}

@router.post("/calculations/{calc_id}/test")
async def test_calculation(
    calc_id: int,
    cycle_filter: Optional[int] = Query(None, description="Cycle to test with"),
    limit: int = Query(10, description="Limit results for testing"),
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db_session),
    dw_db: Session = Depends(get_dw_session)
):
    """Test a calculation and return sample results."""
    
    repo = CalculationRepository(db)
    calculation = repo.get_calculation(calc_id)
    
    if not calculation:
        raise HTTPException(status_code=404, detail="Calculation not found")
    
    # Check permissions
    if calculation.created_by != user_id and not calculation.is_public:
        raise HTTPException(status_code=403, detail="Access denied")
    
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
        raise HTTPException(status