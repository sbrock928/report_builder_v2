"""Dynamic calculation subquery builder for financial data."""

from sqlalchemy import func, and_, or_, case, literal
from sqlalchemy.orm import Session
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass

from app.models.dwh_models import Deal, Tranche, TrancheBal

class CalculationType(Enum):
    """Available calculation types for dropdowns."""
    SUM = "sum"
    AVERAGE = "avg" 
    WEIGHTED_AVERAGE = "weighted_avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    RATIO = "ratio"
    PERCENTAGE = "percentage"

class AggregationLevel(Enum):
    """Level at which to aggregate data."""
    DEAL = "deal"
    TRANCHE = "tranche"

@dataclass
class CalculationConfig:
    """Configuration for a user-defined calculation."""
    name: str
    calculation_type: CalculationType
    target_field: str
    aggregation_level: AggregationLevel
    weight_field: Optional[str] = None  # For weighted averages
    denominator_field: Optional[str] = None  # For ratios/percentages
    filters: Optional[Dict[str, Any]] = None
    cycle_filter: Optional[int] = None  # Specific cycle or latest

class DynamicSubqueryBuilder:
    """Builds dynamic ORM subqueries for custom calculations."""
    
    def __init__(self, session: Session):
        self.session = session
        
        # Map field names to actual model attributes
        self.field_mapping = {
            'ending_balance': 'tr_end_bal_amt',
            'principal_release': 'tr_prin_rel_ls_amt',
            'pass_through_rate': 'tr_pass_thru_rte',
            'accrual_days': 'tr_accrl_days',
            'interest_distribution': 'tr_int_dstrb_amt',
            'principal_distribution': 'tr_prin_dstrb_amt',
            'interest_accrual': 'tr_int_accrl_amt',
            'interest_shortfall': 'tr_int_shtfl_amt',
        }
    
    def build_calculation_subquery(self, config: CalculationConfig):
        """Build a subquery based on calculation configuration."""
        
        # Get the actual field attribute
        target_attr = getattr(TrancheBal, self.field_mapping[config.target_field])
        
        # Start building the query
        if config.aggregation_level == AggregationLevel.DEAL:
            query = self._build_deal_level_query(config, target_attr)
        else:
            query = self._build_tranche_level_query(config, target_attr)
            
        return query.subquery()
    
    def _build_deal_level_query(self, config: CalculationConfig, target_attr):
        """Build deal-level aggregation query."""
        
        # Base selection - always include dl_nbr for joining
        base_select = [TrancheBal.dl_nbr]
        
        # Apply calculation type
        if config.calculation_type == CalculationType.SUM:
            calculation = func.sum(target_attr).label(config.name)
            
        elif config.calculation_type == CalculationType.AVERAGE:
            calculation = func.avg(target_attr).label(config.name)
            
        elif config.calculation_type == CalculationType.WEIGHTED_AVERAGE:
            if not config.weight_field:
                raise ValueError("Weighted average requires weight_field")
            weight_attr = getattr(TrancheBal, self.field_mapping[config.weight_field])
            calculation = (
                func.sum(target_attr * weight_attr) / func.sum(weight_attr)
            ).label(config.name)
            
        elif config.calculation_type == CalculationType.COUNT:
            calculation = func.count(target_attr).label(config.name)
            
        elif config.calculation_type == CalculationType.MIN:
            calculation = func.min(target_attr).label(config.name)
            
        elif config.calculation_type == CalculationType.MAX:
            calculation = func.max(target_attr).label(config.name)
            
        elif config.calculation_type == CalculationType.RATIO:
            if not config.denominator_field:
                raise ValueError("Ratio requires denominator_field")
            denom_attr = getattr(TrancheBal, self.field_mapping[config.denominator_field])
            calculation = (
                func.sum(target_attr) / func.sum(denom_attr)
            ).label(config.name)
            
        elif config.calculation_type == CalculationType.PERCENTAGE:
            if not config.denominator_field:
                raise ValueError("Percentage requires denominator_field")
            denom_attr = getattr(TrancheBal, self.field_mapping[config.denominator_field])
            calculation = (
                (func.sum(target_attr) / func.sum(denom_attr)) * 100
            ).label(config.name)
            
        else:
            raise ValueError(f"Unsupported calculation type: {config.calculation_type}")
        
        base_select.append(calculation)
        
        # Build the query
        query = self.session.query(*base_select)
        
        # Apply filters
        query = self._apply_filters(query, config)
        
        # Group by deal number for aggregation
        query = query.group_by(TrancheBal.dl_nbr)
        
        return query
    
    def _build_tranche_level_query(self, config: CalculationConfig, target_attr):
        """Build tranche-level query (typically for specific cycle data)."""
        
        # Base selection - include tranche identifiers
        base_select = [
            TrancheBal.dl_nbr,
            TrancheBal.tr_id,
            TrancheBal.tr_cusip_id
        ]
        
        # For tranche level, we typically want specific cycle data
        if config.calculation_type in [CalculationType.SUM, CalculationType.RATIO, CalculationType.PERCENTAGE]:
            if config.calculation_type == CalculationType.RATIO:
                denom_attr = getattr(TrancheBal, self.field_mapping[config.denominator_field])
                calculation = (target_attr / denom_attr).label(config.name)
            elif config.calculation_type == CalculationType.PERCENTAGE:
                denom_attr = getattr(TrancheBal, self.field_mapping[config.denominator_field])
                calculation = ((target_attr / denom_attr) * 100).label(config.name)
            else:
                calculation = target_attr.label(config.name)
        else:
            calculation = target_attr.label(config.name)
            
        base_select.append(calculation)
        
        # Build the query
        query = self.session.query(*base_select)
        
        # Apply filters
        query = self._apply_filters(query, config)
        
        return query
    
    def _apply_filters(self, query, config: CalculationConfig):
        """Apply filters to the query."""
        
        # Cycle filter (most common)
        if config.cycle_filter:
            query = query.filter(TrancheBal.cycle_cde == config.cycle_filter)
        
        # Custom filters from config
        if config.filters:
            for field, value in config.filters.items():
                if field in self.field_mapping:
                    attr = getattr(TrancheBal, self.field_mapping[field])
                    if isinstance(value, list):
                        query = query.filter(attr.in_(value))
                    else:
                        query = query.filter(attr == value)
        
        return query
    
    def add_calculation_to_query(self, base_query, config: CalculationConfig):
        """Add a calculation as a left join to an existing query."""
        
        calc_subquery = self.build_calculation_subquery(config)
        
        if config.aggregation_level == AggregationLevel.DEAL:
            # Join on deal number
            join_condition = Deal.dl_nbr == calc_subquery.c.dl_nbr
        else:
            # Join on tranche identifiers
            join_condition = and_(
                Tranche.dl_nbr == calc_subquery.c.dl_nbr,
                Tranche.tr_id == calc_subquery.c.tr_id,
                Tranche.tr_cusip_id == calc_subquery.c.tr_cusip_id
            )
        
        # Add the calculation column to the query
        calculation_column = getattr(calc_subquery.c, config.name)
        base_query = base_query.add_columns(calculation_column)
        
        # Left join the subquery
        base_query = base_query.outerjoin(calc_subquery, join_condition)
        
        return base_query


class CalculationManager:
    """High-level manager for handling multiple calculations."""
    
    def __init__(self, session: Session):
        self.session = session
        self.builder = DynamicSubqueryBuilder(session)
    
    def create_enhanced_query(self, base_model, calculations: List[CalculationConfig]):
        """Create a query with multiple calculations added."""
        
        # Start with base query
        if base_model == "deal":
            query = self.session.query(Deal)
        elif base_model == "tranche":
            query = self.session.query(Tranche)
        else:
            raise ValueError("base_model must be 'deal' or 'tranche'")
        
        # Add each calculation
        for calc_config in calculations:
            query = self.builder.add_calculation_to_query(query, calc_config)
        
        return query
    
    def get_available_fields(self) -> List[str]:
        """Get list of available fields for calculations."""
        return list(self.builder.field_mapping.keys())
    
    def get_calculation_types(self) -> List[str]:
        """Get list of available calculation types."""
        return [calc_type.value for calc_type in CalculationType]