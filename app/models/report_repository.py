"""Repository for managing report configurations."""

from sqlalchemy.orm import Session, joinedload
from typing import List, Optional, Dict, Any
from datetime import datetime

from .reports import Report, ReportDeal, ReportTranche, ReportField, FilterCondition
from .calculations import SavedCalculation


class ReportRepository:
    """Repository for managing report configurations."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create_report(
        self,
        name: str,
        scope: str,
        created_by: str,
        description: Optional[str] = None,
        selected_deals: Optional[List[Dict[str, Any]]] = None,
        selected_fields: Optional[List[Dict[str, Any]]] = None,
        filter_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Report:
        """Create a new report configuration."""
        
        # Create the main report record
        report = Report(
            name=name,
            description=description,
            scope=scope,
            created_by=created_by
        )
        
        self.session.add(report)
        self.session.flush()  # Get the report ID
        
        # Add selected deals and tranches
        if selected_deals:
            for deal_data in selected_deals:
                report_deal = ReportDeal(
                    report_id=report.id,
                    dl_nbr=deal_data['dl_nbr']
                )
                self.session.add(report_deal)
                self.session.flush()  # Get the report_deal ID
                
                # Add selected tranches for this deal
                if 'selected_tranches' in deal_data and deal_data['selected_tranches']:
                    for tranche_data in deal_data['selected_tranches']:
                        report_tranche = ReportTranche(
                            report_deal_id=report_deal.id,
                            dl_nbr=tranche_data['dl_nbr'],
                            tr_id=tranche_data['tr_id']
                        )
                        self.session.add(report_tranche)
        
        # Add selected fields
        if selected_fields:
            for field_data in selected_fields:
                report_field = ReportField(
                    report_id=report.id,
                    field_name=field_data['field_name'],
                    display_name=field_data['display_name'],
                    field_type=field_data['field_type'],
                    field_source=field_data.get('field_source', 'raw_field'),
                    calculation_id=field_data.get('calculation_id'),
                    is_required=field_data.get('is_required', False)
                )
                self.session.add(report_field)
        
        # Add filter conditions
        if filter_conditions:
            for filter_data in filter_conditions:
                filter_condition = FilterCondition(
                    report_id=report.id,
                    field_name=filter_data['field_name'],
                    operator=filter_data['operator'],
                    value=filter_data.get('value')
                )
                self.session.add(filter_condition)
        
        self.session.commit()
        return report
    
    def get_report(self, report_id: int) -> Optional[Report]:
        """Get a report by ID with all related data."""
        return self.session.query(Report).options(
            joinedload(Report.selected_deals).joinedload(ReportDeal.selected_tranches),
            joinedload(Report.selected_fields),
            joinedload(Report.filter_conditions)
        ).filter(
            Report.id == report_id,
            Report.is_active == True
        ).first()
    
    def get_all_reports(self, created_by: Optional[str] = None) -> List[Report]:
        """Get all active reports, optionally filtered by creator."""
        query = self.session.query(Report).filter(Report.is_active == True)
        
        if created_by:
            query = query.filter(Report.created_by == created_by)
        
        return query.order_by(Report.name).all()
    
    def get_report_summaries(self, created_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get lightweight report summaries for listing."""
        query = self.session.query(Report).filter(Report.is_active == True)
        
        if created_by:
            query = query.filter(Report.created_by == created_by)
        
        reports = query.order_by(Report.name).all()
        
        summaries = []
        for report in reports:
            # Count deals and tranches
            deal_count = len(report.selected_deals)
            tranche_count = sum(len(deal.selected_tranches) for deal in report.selected_deals)
            field_count = len(report.selected_fields)
            
            summaries.append({
                'id': report.id,
                'name': report.name,
                'description': report.description,
                'scope': report.scope,
                'created_by': report.created_by,
                'created_date': report.created_date.isoformat(),
                'updated_date': report.updated_date.isoformat(),
                'deal_count': deal_count,
                'tranche_count': tranche_count,
                'field_count': field_count
            })
        
        return summaries
    
    def update_report(
        self,
        report_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        scope: Optional[str] = None,
        selected_deals: Optional[List[Dict[str, Any]]] = None,
        selected_fields: Optional[List[Dict[str, Any]]] = None,
        filter_conditions: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Report]:
        """Update an existing report configuration."""
        
        report = self.get_report(report_id)
        if not report:
            return None
        
        # Update basic fields
        if name is not None:
            report.name = name
        if description is not None:
            report.description = description
        if scope is not None:
            report.scope = scope
        
        report.updated_date = datetime.now()
        
        # Update deals and tranches if provided
        if selected_deals is not None:
            # Remove existing deals (cascade will remove tranches)
            for deal in report.selected_deals:
                self.session.delete(deal)
            self.session.flush()
            
            # Add new deals
            for deal_data in selected_deals:
                report_deal = ReportDeal(
                    report_id=report.id,
                    dl_nbr=deal_data['dl_nbr']
                )
                self.session.add(report_deal)
                self.session.flush()
                
                # Add tranches for this deal
                if 'selected_tranches' in deal_data and deal_data['selected_tranches']:
                    for tranche_data in deal_data['selected_tranches']:
                        report_tranche = ReportTranche(
                            report_deal_id=report_deal.id,
                            dl_nbr=tranche_data['dl_nbr'],
                            tr_id=tranche_data['tr_id']
                        )
                        self.session.add(report_tranche)
        
        # Update fields if provided
        if selected_fields is not None:
            # Remove existing fields
            for field in report.selected_fields:
                self.session.delete(field)
            self.session.flush()
            
            # Add new fields
            for field_data in selected_fields:
                report_field = ReportField(
                    report_id=report.id,
                    field_name=field_data['field_name'],
                    display_name=field_data['display_name'],
                    field_type=field_data['field_type'],
                    field_source=field_data.get('field_source', 'raw_field'),
                    calculation_id=field_data.get('calculation_id'),
                    is_required=field_data.get('is_required', False)
                )
                self.session.add(report_field)
        
        # Update filter conditions if provided
        if filter_conditions is not None:
            # Remove existing filter conditions
            for condition in report.filter_conditions:
                self.session.delete(condition)
            self.session.flush()
            
            # Add new filter conditions
            for filter_data in filter_conditions:
                filter_condition = FilterCondition(
                    report_id=report.id,
                    field_name=filter_data['field_name'],
                    operator=filter_data['operator'],
                    value=filter_data.get('value')
                )
                self.session.add(filter_condition)
        
        self.session.commit()
        return report
    
    def delete_report(self, report_id: int) -> bool:
        """Soft delete a report."""
        report = self.get_report(report_id)
        if not report:
            return False
        
        report.is_active = False
        report.updated_date = datetime.now()
        self.session.commit()
        return True
    
    def search_reports(self, search_term: str, created_by: Optional[str] = None) -> List[Report]:
        """Search reports by name or description."""
        query = self.session.query(Report).filter(Report.is_active == True)
        
        if created_by:
            query = query.filter(Report.created_by == created_by)
        
        # Search in name and description
        search_filter = Report.name.ilike(f'%{search_term}%')
        if search_term:
            search_filter = search_filter | Report.description.ilike(f'%{search_term}%')
        
        query = query.filter(search_filter)
        return query.order_by(Report.name).all()
    
    def get_report_deals(self, report_id: int) -> List[int]:
        """Get list of deal numbers for a report."""
        report = self.get_report(report_id)
        if not report:
            return []
        
        return [deal.dl_nbr for deal in report.selected_deals]
    
    def get_report_tranches(self, report_id: int) -> Dict[int, List[str]]:
        """Get tranches grouped by deal for a report."""
        report = self.get_report(report_id)
        if not report:
            return {}
        
        tranches_by_deal = {}
        for deal in report.selected_deals:
            tranches_by_deal[deal.dl_nbr] = [
                tranche.tr_id for tranche in deal.selected_tranches
            ]
        
        return tranches_by_deal
    
    def get_saved_calculations_for_report(self, report_id: int) -> List[SavedCalculation]:
        """Get saved calculations referenced by a report."""
        report = self.get_report(report_id)
        if not report:
            return []
        
        calc_ids = [
            field.calculation_id for field in report.selected_fields 
            if field.field_source == 'saved_calculation' and field.calculation_id
        ]
        
        if not calc_ids:
            return []
        
        return self.session.query(SavedCalculation).filter(
            SavedCalculation.id.in_(calc_ids),
            SavedCalculation.is_active == True
        ).all()