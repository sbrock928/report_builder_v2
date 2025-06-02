"""Database models for persisting user-defined calculations."""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.core.database import Base
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel

class SavedCalculation(Base):
    """Stores user-defined calculations for reuse."""
    __tablename__ = "saved_calculations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Calculation configuration
    calculation_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'sum', 'avg', etc.
    target_field: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregation_level: Mapped[str] = mapped_column(String(20), nullable=False)  # 'deal' or 'tranche'
    
    # Optional fields
    weight_field: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    denominator_field: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    cycle_filter: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Filters stored as JSON
    filters: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    def to_calculation_config(self):
        """Convert database record to CalculationConfig object."""
        from app.services.calculation_builder import CalculationConfig, CalculationType, AggregationLevel
        
        return CalculationConfig(
            name=self.name,
            calculation_type=CalculationType(self.calculation_type),
            target_field=self.target_field,
            aggregation_level=AggregationLevel(self.aggregation_level),
            weight_field=self.weight_field,
            denominator_field=self.denominator_field,
            filters=self.filters,
            cycle_filter=self.cycle_filter
        )
    
    @classmethod
    def from_calculation_config(cls, config, name: str, description: str = None):
        """Create database record from CalculationConfig object."""
        return cls(
            name=name,
            description=description,
            calculation_type=config.calculation_type.value,
            target_field=config.target_field,
            aggregation_level=config.aggregation_level.value,
            weight_field=config.weight_field,
            denominator_field=config.denominator_field,
            cycle_filter=config.cycle_filter,
            filters=config.filters
        )

class CalculationRepository:
    """Repository for managing saved calculations."""
    
    def __init__(self, session):
        self.session = session
    
    def save_calculation(self, config, name: str, description: str = None):
        """Save a calculation configuration."""
        saved_calc = SavedCalculation.from_calculation_config(
            config, name, description
        )
        self.session.add(saved_calc)
        self.session.commit()
        return saved_calc
    
    def get_calculation(self, calc_id: int):
        """Get a saved calculation by ID."""
        return self.session.query(SavedCalculation).filter(
            SavedCalculation.id == calc_id,
            SavedCalculation.is_active == True
        ).first()
    
    def get_all_calculations(self):
        """Get all active calculations."""
        return self.session.query(SavedCalculation).filter(
            SavedCalculation.is_active == True
        ).order_by(SavedCalculation.name).all()
    
    def update_calculation(self, calc_id: int, **updates):
        """Update a saved calculation."""
        calc = self.get_calculation(calc_id)
        if calc:
            for key, value in updates.items():
                if hasattr(calc, key):
                    setattr(calc, key, value)
            calc.updated_at = func.now()
            self.session.commit()
        return calc
    
    def delete_calculation(self, calc_id: int):
        """Soft delete a calculation."""
        calc = self.get_calculation(calc_id)
        if calc:
            calc.is_active = False
            self.session.commit()
        return calc
    
    def search_calculations(self, search_term: str):
        """Search calculations by name or description."""
        query = self.session.query(SavedCalculation).filter(
            SavedCalculation.is_active == True
        )
        
        # Search in name and description
        search_filter = SavedCalculation.name.ilike(f'%{search_term}%')
        if hasattr(SavedCalculation, 'description'):
            search_filter = search_filter | SavedCalculation.description.ilike(f'%{search_term}%')
        
        query = query.filter(search_filter)
        return query.order_by(SavedCalculation.name).all()

# API Models for FastAPI
class CalculationConfigRequest(BaseModel):
    """Request model for creating calculations via API."""
    name: str
    description: Optional[str] = None
    calculation_type: str
    target_field: str
    aggregation_level: str
    weight_field: Optional[str] = None
    denominator_field: Optional[str] = None
    cycle_filter: Optional[int] = None
    filters: Optional[Dict[str, Any]] = None

class CalculationConfigResponse(BaseModel):
    """Response model for calculation data."""
    id: int
    name: str
    description: Optional[str]
    calculation_type: str
    target_field: str
    aggregation_level: str
    weight_field: Optional[str]
    denominator_field: Optional[str]
    cycle_filter: Optional[int]
    filters: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime

class DropdownOption(BaseModel):
    """Model for dropdown options."""
    value: str
    label: str
    description: Optional[str] = None

class DropdownData(BaseModel):
    """All dropdown data for the UI."""
    calculation_types: list[DropdownOption]
    target_fields: list[DropdownOption]
    aggregation_levels: list[DropdownOption]
    saved_calculations: list[CalculationConfigResponse]