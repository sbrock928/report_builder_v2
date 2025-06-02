"""Service for executing report configurations."""

from sqlalchemy import func, and_, or_, case, literal, text
from sqlalchemy.orm import Session, aliased
from typing import Dict, List, Optional, Any, Union
import time
from datetime import datetime

from app.models.reports import Report, ReportDeal, ReportTranche, ReportField, FilterCondition
from app.models.report_repository import ReportRepository
from app.models.calculations import SavedCalculation, CalculationRepository
from app.models.dwh_models import Deal, Tranche, TrancheBal
from app.services.calculation_builder import (
    DynamicSubqueryBuilder, CalculationManager, CalculationConfig,
    CalculationType, AggregationLevel
)
from app.models.report_api_models import (
    ReportExecutionResult, ReportColumn, FilterOperator, FieldSource
)


class ReportExecutionService:
    """Service for executing report configurations and generating results."""
    
    def __init__(self, app_db: Session, dw_db: Session):
        self.app_db = app_db
        self.dw_db = dw_db
        self.repo = ReportRepository(app_db)
        self.calc_repo = CalculationRepository(app_db)
        self.calc_builder = DynamicSubqueryBuilder(dw_db)
        self.calc_manager = CalculationManager(dw_db)
        
        # Field mapping for raw fields
        self.field_mapping = {
            # Deal fields
            'dl_nbr': Deal.dl_nbr,
            'issr_cde': Deal.issr_cde,
            'cdi_file_nme': Deal.cdi_file_nme,
            'CDB_cdi_file_nme': Deal.CDB_cdi_file_nme,
            # Tranche fields
            'tr_id': Tranche.tr_id,
            'tr_cusip_id': Tranche.tr_cusip_id,
            # TrancheBal fields
            'cycle_cde': TrancheBal.cycle_cde,
            'tr_end_bal_amt': TrancheBal.tr_end_bal_amt,
            'tr_prin_rel_ls_amt': TrancheBal.tr_prin_rel_ls_amt,
            'tr_pass_thru_rte': TrancheBal.tr_pass_thru_rte,
            'tr_accrl_days': TrancheBal.tr_accrl_days,
            'tr_int_dstrb_amt': TrancheBal.tr_int_dstrb_amt,
            'tr_prin_dstrb_amt': TrancheBal.tr_prin_dstrb_amt,
            'tr_int_accrl_amt': TrancheBal.tr_int_accrl_amt,
            'tr_int_shtfl_amt': TrancheBal.tr_int_shtfl_amt,
        }
    
    def execute_report(
        self, 
        report_id: int, 
        cycle_filter: Optional[int] = None,
        additional_filters: Optional[List[Dict[str, Any]]] = None
    ) -> ReportExecutionResult:
        """Execute a report configuration and return results."""
        
        start_time = time.time()
        
        # Get the report configuration
        report = self.repo.get_report(report_id)
        if not report:
            raise ValueError(f"Report {report_id} not found")
        
        # Build and execute the query
        if report.scope == "DEAL":
            results = self._execute_deal_level_report(report, cycle_filter, additional_filters)
        else:
            results = self._execute_tranche_level_report(report, cycle_filter, additional_filters)
        
        # Convert results to response format
        columns = self._get_report_columns(report)
        rows = self._format_results(results, report)
        
        execution_time = int((time.time() - start_time) * 1000)
        
        return ReportExecutionResult(
            report_id=report.id,
            report_name=report.name,
            scope=report.scope,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=execution_time,
            cycle_filter=cycle_filter
        )
    
    def _execute_deal_level_report(
        self, 
        report: Report, 
        cycle_filter: Optional[int],
        additional_filters: Optional[List[Dict[str, Any]]]
    ) -> List[Any]:
        """Execute a deal-level report."""
        
        # Start with base deal query
        query = self.dw_db.query(Deal)
        
        # Filter by selected deals
        if report.selected_deals:
            deal_numbers = [deal.dl_nbr for deal in report.selected_deals]
            query = query.filter(Deal.dl_nbr.in_(deal_numbers))
        
        # Get field types
        raw_fields = [field for field in report.selected_fields if field.field_source == "raw_field"]
        calculation_fields = [field for field in report.selected_fields if field.field_source == "saved_calculation"]
        
        # Build selection items for raw fields
        select_items = []
        
        # Check if we need tranche data
        needs_tranche_data = any(
            field.field_name.startswith('tr_') for field in raw_fields
        ) or len(calculation_fields) > 0
        
        if needs_tranche_data:
            # Join with Tranche and TrancheBal for aggregation
            query = query.join(Tranche, Deal.dl_nbr == Tranche.dl_nbr)
            query = query.join(TrancheBal, 
                              and_(Tranche.dl_nbr == TrancheBal.dl_nbr,
                                   Tranche.tr_id == TrancheBal.tr_id))
            
            # Apply cycle filter
            if cycle_filter:
                query = query.filter(TrancheBal.cycle_cde == cycle_filter)
            
            # Apply tranche filtering if specified for deal-level reports
            if report.selected_deals:
                for deal_config in report.selected_deals:
                    if deal_config.selected_tranches:
                        tranche_ids = [t.tr_id for t in deal_config.selected_tranches]
                        if tranche_ids:  # Only filter if specific tranches selected
                            query = query.filter(
                                or_(
                                    TrancheBal.dl_nbr != deal_config.dl_nbr,
                                    TrancheBal.tr_id.in_(tranche_ids)
                                )
                            )
            
            # Add deal-level fields first
            for field in raw_fields:
                if field.field_name in ['dl_nbr', 'issr_cde', 'cdi_file_nme', 'CDB_cdi_file_nme']:
                    db_field = self.field_mapping[field.field_name]
                    select_items.append(db_field.label(field.field_name))
            
            # Add aggregated financial fields
            for field in raw_fields:
                if field.field_name.startswith('tr_') and field.field_name != 'tr_id':
                    db_field = self.field_mapping.get(field.field_name)
                    if db_field is not None:
                        if field.field_name in ['tr_end_bal_amt', 'tr_prin_dstrb_amt', 'tr_int_dstrb_amt', 'tr_int_accrl_amt', 'tr_int_shtfl_amt']:
                            # Sum these fields
                            select_items.append(func.sum(db_field).label(field.field_name))
                        elif field.field_name == 'tr_pass_thru_rte':
                            # Weighted average by balance
                            select_items.append(
                                (func.sum(db_field * TrancheBal.tr_end_bal_amt) / 
                                 func.nullif(func.sum(TrancheBal.tr_end_bal_amt), 0)).label(field.field_name)
                            )
                        elif field.field_name == 'tr_accrl_days':
                            # Average for days
                            select_items.append(func.avg(db_field).label(field.field_name))
                        else:
                            # Default to sum for other numeric fields
                            select_items.append(func.sum(db_field).label(field.field_name))
            
            # Group by deal-level fields
            deal_group_fields = []
            for field in raw_fields:
                if field.field_name in ['dl_nbr', 'issr_cde', 'cdi_file_nme', 'CDB_cdi_file_nme']:
                    db_field = self.field_mapping[field.field_name]
                    deal_group_fields.append(db_field)
            
            if deal_group_fields:
                query = query.group_by(*deal_group_fields)
            else:
                query = query.group_by(Deal.dl_nbr)
        else:
            # Simple deal-only query
            for field in raw_fields:
                if field.field_name in self.field_mapping:
                    db_field = self.field_mapping[field.field_name]
                    select_items.append(db_field.label(field.field_name))
        
        # Set the selection if we have items
        if select_items:
            query = query.with_entities(*select_items)
        
        # Apply additional filters
        query = self._apply_filters(query, report.filter_conditions, additional_filters, cycle_filter)
        
        return query.all()
    
    def _execute_tranche_level_report(
        self, 
        report: Report, 
        cycle_filter: Optional[int],
        additional_filters: Optional[List[Dict[str, Any]]]
    ) -> List[Any]:
        """Execute a tranche-level report."""
        
        # Start with base query joining all required tables
        query = self.dw_db.query(Deal, Tranche, TrancheBal).join(
            Tranche, Deal.dl_nbr == Tranche.dl_nbr
        ).join(
            TrancheBal, and_(
                Tranche.dl_nbr == TrancheBal.dl_nbr,
                Tranche.tr_id == TrancheBal.tr_id
            )
        )
        
        # Apply cycle filter
        if cycle_filter:
            query = query.filter(TrancheBal.cycle_cde == cycle_filter)
        
        # Filter by selected deals and tranches
        if report.selected_deals:
            deal_filters = []
            for deal_config in report.selected_deals:
                if deal_config.selected_tranches:
                    # Specific tranches selected
                    tranche_ids = [t.tr_id for t in deal_config.selected_tranches]
                    deal_filters.append(
                        and_(
                            TrancheBal.dl_nbr == deal_config.dl_nbr,
                            TrancheBal.tr_id.in_(tranche_ids)
                        )
                    )
                else:
                    # All tranches for this deal
                    deal_filters.append(TrancheBal.dl_nbr == deal_config.dl_nbr)
            
            if deal_filters:
                query = query.filter(or_(*deal_filters))
        
        # Build selection with raw fields
        select_items = []
        raw_fields = [field for field in report.selected_fields if field.field_source == "raw_field"]
        calculation_fields = [field for field in report.selected_fields if field.field_source == "saved_calculation"]
        
        for field in raw_fields:
            if field.field_name in self.field_mapping:
                db_field = self.field_mapping[field.field_name]
                select_items.append(db_field.label(field.field_name))
        
        # Set the selection
        if select_items:
            query = query.with_entities(*select_items)
        
        # Apply additional filters
        query = self._apply_filters(query, report.filter_conditions, additional_filters, cycle_filter)
        
        return query.all()
    
    def _apply_filters(
        self, 
        query, 
        filter_conditions: List[FilterCondition],
        additional_filters: Optional[List[Dict[str, Any]]],
        cycle_filter: Optional[int]
    ):
        """Apply filter conditions to the query."""
        
        # Apply stored filter conditions
        for condition in filter_conditions:
            if condition.field_name in self.field_mapping:
                db_field = self.field_mapping[condition.field_name]
                filter_clause = self._build_filter_clause(db_field, condition.operator, condition.value)
                if filter_clause is not None:
                    query = query.filter(filter_clause)
        
        # Apply additional runtime filters
        if additional_filters:
            for filter_data in additional_filters:
                field_name = filter_data.get('field_name')
                operator = filter_data.get('operator')
                value = filter_data.get('value')
                
                if field_name in self.field_mapping:
                    db_field = self.field_mapping[field_name]
                    filter_clause = self._build_filter_clause(db_field, operator, value)
                    if filter_clause is not None:
                        query = query.filter(filter_clause)
        
        return query
    
    def _build_filter_clause(self, db_field, operator: str, value: Any):
        """Build a filter clause for the given field, operator, and value."""
        
        if operator == "equals":
            return db_field == value
        elif operator == "not_equals":
            return db_field != value
        elif operator == "greater_than":
            return db_field > value
        elif operator == "less_than":
            return db_field < value
        elif operator == "greater_than_or_equal":
            return db_field >= value
        elif operator == "less_than_or_equal":
            return db_field <= value
        elif operator == "in":
            if isinstance(value, str):
                # Parse comma-separated values
                values = [v.strip() for v in value.split(',')]
                return db_field.in_(values)
            elif isinstance(value, list):
                return db_field.in_(value)
        elif operator == "not_in":
            if isinstance(value, str):
                values = [v.strip() for v in value.split(',')]
                return ~db_field.in_(values)
            elif isinstance(value, list):
                return ~db_field.in_(value)
        elif operator == "contains":
            return db_field.ilike(f'%{value}%')
        elif operator == "not_contains":
            return ~db_field.ilike(f'%{value}%')
        elif operator == "is_null":
            return db_field.is_(None)
        elif operator == "is_not_null":
            return db_field.isnot(None)
        
        return None
    
    def _get_report_columns(self, report: Report) -> List[ReportColumn]:
        """Get column definitions for the report."""
        
        columns = []
        for field in report.selected_fields:
            columns.append(
                ReportColumn(
                    field=field.field_name,
                    header=field.display_name,
                    type=field.field_type
                )
            )
        
        return columns
    
    def _format_results(self, results: List[Any], report: Report) -> List[Dict[str, Any]]:
        """Format query results into a list of dictionaries."""
        
        if not results:
            return []
        
        formatted_rows = []
        
        for row in results:
            row_dict = {}
            
            # Handle different result types
            if hasattr(row, '_fields'):
                # SQLAlchemy Row object
                for field in row._fields:
                    value = getattr(row, field)
                    row_dict[field] = self._format_value(value)
            elif hasattr(row, '__dict__'):
                # SQLAlchemy model instance
                for field in report.selected_fields:
                    if hasattr(row, field.field_name):
                        value = getattr(row, field.field_name)
                        row_dict[field.field_name] = self._format_value(value)
            else:
                # Tuple or other format - handle by field order
                for i, field in enumerate(report.selected_fields):
                    try:
                        value = row[i] if hasattr(row, '__getitem__') and i < len(row) else None
                        row_dict[field.field_name] = self._format_value(value)
                    except (IndexError, TypeError):
                        row_dict[field.field_name] = None
            
            formatted_rows.append(row_dict)
        
        return formatted_rows
    
    def _format_value(self, value: Any) -> Any:
        """Format a single value for JSON serialization."""
        
        if value is None:
            return None
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)