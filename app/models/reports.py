"""Database models for report wizard functionality."""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from app.core.database import Base
from typing import Optional, List
from datetime import datetime


class Report(Base):
    """Report configuration model stored in config database."""
    
    __tablename__ = "reports"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)  # 'DEAL' or 'TRANCHE'
    created_by: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    created_date: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_date: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    selected_deals = relationship("ReportDeal", back_populates="report", cascade="all, delete-orphan")
    selected_fields = relationship("ReportField", back_populates="report", cascade="all, delete-orphan")
    filter_conditions = relationship("FilterCondition", back_populates="report", cascade="all, delete-orphan")


class ReportDeal(Base):
    """Report deal association model - stores which deals are selected for a report."""
    
    __tablename__ = "report_deals"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_id: Mapped[int] = mapped_column(Integer, ForeignKey("reports.id"), nullable=False)
    dl_nbr: Mapped[int] = mapped_column(Integer, nullable=False)  # References data warehouse deal dl_nbr
    
    # Relationships
    report = relationship("Report", back_populates="selected_deals")
    selected_tranches = relationship("ReportTranche", back_populates="report_deal", cascade="all, delete-orphan")


class ReportTranche(Base):
    """Report tranche association model - stores which tranches are selected for a report."""
    
    __tablename__ = "report_tranches"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_deal_id: Mapped[int] = mapped_column(Integer, ForeignKey("report_deals.id"), nullable=False)
    dl_nbr: Mapped[int] = mapped_column(Integer, nullable=False)  # References data warehouse tranche dl_nbr
    tr_id: Mapped[str] = mapped_column(String(15), nullable=False)  # References data warehouse tranche tr_id
    
    # Relationship
    report_deal = relationship("ReportDeal", back_populates="selected_tranches")


class ReportField(Base):
    """Report field configuration model - stores which fields/calculations are selected for a report."""
    
    __tablename__ = "report_fields"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_id: Mapped[int] = mapped_column(Integer, ForeignKey("reports.id"), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "dl_nbr", "calc_name"
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "Deal Number", "Total Principal"
    field_type: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g., "text", "number", "date", "percentage"
    field_source: Mapped[str] = mapped_column(String(20), nullable=False)  # "raw_field" or "saved_calculation"
    calculation_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # FK to SavedCalculation if field_source='saved_calculation'
    is_required: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Relationship
    report = relationship("Report", back_populates="selected_fields")


class FilterCondition(Base):
    """Filter condition model for reports."""
    
    __tablename__ = "filter_conditions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    report_id: Mapped[int] = mapped_column(Integer, ForeignKey("reports.id"), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "cycle_cde", "dl_nbr"
    operator: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g., "equals", "greater_than", "in"
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # Stored as string, parsed based on field type
    
    # Relationship
    report = relationship("Report", back_populates="filter_conditions")