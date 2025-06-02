"""Database models for the datawarehouse module (data warehouse database)."""

from sqlalchemy import (
    Column, Integer, String, Float, SmallInteger, ForeignKey, CHAR, and_, Numeric
)
from sqlalchemy.orm import relationship, Mapped, mapped_column, foreign
from sqlalchemy.dialects.mssql import MONEY
from app.core.database import DWBase as Base

class Deal(Base):
    """Deal model for securities stored in data warehouse."""
    __tablename__ = "deal"
    
    dl_nbr = Column(Integer, primary_key=True)
    issr_cde = Column(CHAR(12), nullable=False)
    cdi_file_nme = Column(CHAR(8), nullable=False)
    CDB_cdi_file_nme = Column(CHAR(10), nullable=True)
    
    tranches = relationship("Tranche", back_populates="deal")

class Tranche(Base):
    """Tranche model - child securities of a Deal."""
    __tablename__ = "tranche"
    
    dl_nbr: Mapped[int] = mapped_column(ForeignKey("deal.dl_nbr"), primary_key=True)
    tr_id: Mapped[str] = mapped_column(String(15), primary_key=True)
    tr_cusip_id: Mapped[str] = mapped_column(String(14), primary_key=True)
    
    deal = relationship("Deal", back_populates="tranches")
    
    tranchebals = relationship(
        "TrancheBal",
        back_populates="tranche",
        primaryjoin=lambda: and_(
            foreign(Tranche.dl_nbr) == TrancheBal.dl_nbr,
            foreign(Tranche.tr_id) == TrancheBal.tr_id,
        ),
        foreign_keys=lambda: [TrancheBal.dl_nbr, TrancheBal.tr_id],
        overlaps="deal,tranches",
    )

class TrancheBal(Base):
    """Tranche balance/historical data."""
    __tablename__ = "tranchebal"
    
    dl_nbr: Mapped[int] = mapped_column(ForeignKey("tranche.dl_nbr"), primary_key=True)
    tr_id: Mapped[str] = mapped_column(String(15), ForeignKey("tranche.tr_id"), primary_key=True)
    cycle_cde: Mapped[SmallInteger] = mapped_column(SmallInteger, nullable=False, primary_key=True)  # YYYY-MM-DD format
    
    # Newly added fields
    tr_end_bal_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    tr_prin_rel_ls_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    tr_pass_thru_rte: Mapped[float] = mapped_column(Float(53), nullable=False)
    tr_accrl_days: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    tr_int_dstrb_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    tr_prin_dstrb_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    tr_int_accrl_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    tr_int_shtfl_amt: Mapped[float] = mapped_column(Numeric(19, 4), nullable=False)
    
    tranche = relationship(
        "Tranche",
        back_populates="tranchebals",
        primaryjoin=lambda: and_(
            TrancheBal.dl_nbr == foreign(Tranche.dl_nbr),
            TrancheBal.tr_id == foreign(Tranche.tr_id),
        ),
        foreign_keys=lambda: [TrancheBal.dl_nbr, TrancheBal.tr_id],
        overlaps="deal,tranches",
    )