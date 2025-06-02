"""Database models for report management."""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.core.database import Base
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel

class SavedReport(Base):
    """Stores user-defined report templates."""
    __tablename__ = "saved_reports"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Report configuration
    aggregation_level: Mapped[str] = mapped_column(String(20), nullable=False)  # 'deal' or 'tranche'
    calculation_ids: Mapped[List[int]] = mapped_column(JSON, nullable=False)  # Array of calculation IDs
    
    # Report filters
    cycle_filter: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    additional_filters: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

class ReportRepository:
    """Repository for managing saved reports."""
    
    def __init__(self, session):
        self.session = session
    
    def save_report(self, name: str, description: str = None, aggregation_level: str = None,
                   calculation_ids: List[int] = None, cycle_filter: int = None, 
                   additional_filters: dict = None):
        """Save a report template."""
        saved_report = SavedReport(
            name=name,
            description=description,
            aggregation_level=aggregation_level,
            calculation_ids=calculation_ids or [],
            cycle_filter=cycle_filter,
            additional_filters=additional_filters
        )
        self.session.add(saved_report)
        self.session.commit()
        return saved_report
    
    def get_report(self, report_id: int):
        """Get a saved report by ID."""
        return self.session.query(SavedReport).filter(
            SavedReport.id == report_id,
            SavedReport.is_active == True
        ).first()
    
    def get_all_reports(self):
        """Get all active reports."""
        return self.session.query(SavedReport).filter(
            SavedReport.is_active == True
        ).order_by(SavedReport.name).all()
    
    def update_report(self, report_id: int, **updates):
        """Update a saved report."""
        report = self.get_report(report_id)
        if report:
            for key, value in updates.items():
                if hasattr(report, key):
                    setattr(report, key, value)
            report.updated_at = func.now()
            self.session.commit()
        return report
    
    def delete_report(self, report_id: int):
        """Soft delete a report."""
        report = self.get_report(report_id)
        if report:
            report.is_active = False
            self.session.commit()
        return report
    
    def search_reports(self, search_term: str):
        """Search reports by name or description."""
        query = self.session.query(SavedReport).filter(
            SavedReport.is_active == True
        )
        
        # Search in name and description
        search_filter = SavedReport.name.ilike(f'%{search_term}%')
        if hasattr(SavedReport, 'description'):
            search_filter = search_filter | SavedReport.description.ilike(f'%{search_term}%')
        
        query = query.filter(search_filter)
        return query.order_by(SavedReport.name).all()

# API Models for FastAPI
class ReportConfigRequest(BaseModel):
    """Request model for creating reports via API."""
    name: str
    description: Optional[str] = None
    aggregation_level: str
    calculation_ids: List[int]
    cycle_filter: Optional[int] = None
    additional_filters: Optional[Dict[str, Any]] = None

class ReportConfigResponse(BaseModel):
    """Response model for report data."""
    id: int
    name: str
    description: Optional[str]
    aggregation_level: str
    calculation_ids: List[int]
    cycle_filter: Optional[int]
    additional_filters: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime

class ReportDropdownData(BaseModel):
    """Dropdown data for report builder."""
    available_calculations: List[Dict[str, Any]]
    aggregation_levels: List[Dict[str, str]]
    saved_reports: List[ReportConfigResponse]