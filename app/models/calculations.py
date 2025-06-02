"""Database models for persisting user-defined calculations."""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, JSON, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from app.core.database import Base
from typing import Optional, Dict, Any
import json
from datetime import datetime
from pydantic import BaseModel

class SavedCalculation(Base):
    """Stores user-defined calculations for reuse."""
    __tablename__ = "saved_calculations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Calculation configuration stored as JSON
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
    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # User ID or name
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Make this calculation available to all users or just the creator
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    
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
    def from_calculation_config(cls, config, name: str, description: str = None, 
                              created_by: str = None, is_public: bool = False):
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
            filters=config.filters,
            created_by=created_by,
            is_public=is_public
        )

class CalculationTemplate(Base):
    """Pre-defined calculation templates for common use cases."""
    __tablename__ = "calculation_templates"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)  # 'cashflow', 'performance', etc.
    
    # Same config fields as SavedCalculation
    calculation_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_field: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregation_level: Mapped[str] = mapped_column(String(20), nullable=False)
    weight_field: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    denominator_field: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Templates don't have cycle filters by default
    default_filters: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)

class CalculationRepository:
    """Repository for managing saved calculations."""
    
    def __init__(self, session):
        self.session = session
    
    def save_calculation(self, config, name: str, description: str = None, 
                        created_by: str = None, is_public: bool = False):
        """Save a calculation configuration."""
        saved_calc = SavedCalculation.from_calculation_config(
            config, name, description, created_by, is_public
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
    
    def get_user_calculations(self, user_id: str):
        """Get all calculations for a specific user."""
        return self.session.query(SavedCalculation).filter(
            SavedCalculation.created_by == user_id,
            SavedCalculation.is_active == True
        ).order_by(SavedCalculation.name).all()
    
    def get_public_calculations(self):
        """Get all public calculations."""
        return self.session.query(SavedCalculation).filter(
            SavedCalculation.is_public == True,
            SavedCalculation.is_active == True
        ).order_by(SavedCalculation.name).all()
    
    def get_calculation_templates(self, category: str = None):
        """Get calculation templates, optionally filtered by category."""
        query = self.session.query(CalculationTemplate).filter(
            CalculationTemplate.is_active == True
        )
        if category:
            query = query.filter(CalculationTemplate.category == category)
        return query.order_by(CalculationTemplate.sort_order, CalculationTemplate.name).all()
    
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
    
    def search_calculations(self, search_term: str, user_id: str = None):
        """Search calculations by name or description."""
        query = self.session.query(SavedCalculation).filter(
            SavedCalculation.is_active == True
        )
        
        # Search in name and description
        search_filter = SavedCalculation.name.ilike(f'%{search_term}%')
        if hasattr(SavedCalculation, 'description'):
            search_filter = search_filter | SavedCalculation.description.ilike(f'%{search_term}%')
        
        query = query.filter(search_filter)
        
        # Filter by user's calculations or public ones
        if user_id:
            query = query.filter(
                (SavedCalculation.created_by == user_id) |
                (SavedCalculation.is_public == True)
            )
        else:
            query = query.filter(SavedCalculation.is_public == True)
        
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
    is_public: bool = False

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
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime
    is_public: bool

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
    templates: list[CalculationConfigResponse]