"""Database setup script to create tables and seed initial data."""

from app.core.database import create_tables, create_dw_tables, DWSessionLocal
from app.models.dwh_models import Deal, Tranche, TrancheBal
import random
from datetime import datetime

def seed_sample_dw_data():
    """Seed sample data warehouse data for testing."""
    session = DWSessionLocal()
    
    # Check if data already exists
    existing_deals = session.query(Deal).count()
    if existing_deals > 0:
        print(f"- Data warehouse already has {existing_deals} deals")
        session.close()
        return
    
    print("🏗️ Creating sample data warehouse data...")
    
    # Create sample deals
    deals = []
    for i in range(1, 6):  # 5 deals
        deal = Deal(
            dl_nbr=12340 + i,
            issr_cde=f"ISSUER{i:02d}",
            cdi_file_nme=f"CDI{i:05d}",
            CDB_cdi_file_nme=f"CDB{i:07d}" if i % 2 == 0 else None
        )
        deals.append(deal)
        session.add(deal)
    
    session.commit()
    print(f"✓ Created {len(deals)} sample deals")
    
    # Create sample tranches
    tranches = []
    tranche_types = ['A1', 'A2', 'A3', 'B1', 'B2']
    
    for deal in deals:
        for j, tr_type in enumerate(tranche_types[:3]):  # 3 tranches per deal
            tranche = Tranche(
                dl_nbr=deal.dl_nbr,
                tr_id=tr_type,
                tr_cusip_id=f"{deal.dl_nbr}{tr_type}{j:06d}"
            )
            tranches.append(tranche)
            session.add(tranche)
    
    session.commit()
    print(f"✓ Created {len(tranches)} sample tranches")
    
    # Create sample tranche balance data
    cycles = [202410, 202411, 202412]  # Oct, Nov, Dec 2024
    tranche_bals = []
    
    for tranche in tranches:
        for cycle in cycles:
            # Generate realistic financial data
            base_balance = random.uniform(1000000, 10000000)  # $1M-$10M
            
            tranche_bal = TrancheBal(
                dl_nbr=tranche.dl_nbr,
                tr_id=tranche.tr_id,
                cycle_cde=cycle,
                tr_end_bal_amt=base_balance,
                tr_prin_rel_ls_amt=random.uniform(0, base_balance * 0.01),  # 0-1% loss
                tr_pass_thru_rte=random.uniform(0.03, 0.08),  # 3-8% rate
                tr_accrl_days=30 if cycle % 2 == 0 else 31,  # Days in month
                tr_int_dstrb_amt=base_balance * random.uniform(0.003, 0.007),  # Monthly interest
                tr_prin_dstrb_amt=base_balance * random.uniform(0.01, 0.05),  # Monthly principal
                tr_int_accrl_amt=base_balance * random.uniform(0.0025, 0.0075),  # Accrued interest
                tr_int_shtfl_amt=random.uniform(0, base_balance * 0.001)  # Small shortfall
            )
            tranche_bals.append(tranche_bal)
            session.add(tranche_bal)
    
    session.commit()
    session.close()
    print(f"✓ Created {len(tranche_bals)} sample tranche balance records")

def main():
    """Main setup function."""
    print("🚀 Setting up Financial Calculations Database")
    print("=" * 50)
    
    # Create main application tables
    print("📊 Creating main application tables...")
    create_tables()
    print("✓ Main application tables created")
    
    # Create data warehouse tables (for demo)
    print("🏪 Creating data warehouse tables...")
    create_dw_tables()
    print("✓ Data warehouse tables created")
    
    # Seed sample data warehouse data
    print("🌱 Seeding sample data warehouse data...")
    seed_sample_dw_data()
    
    print("\n✅ Database setup completed successfully!")
    print("\nNext steps:")
    print("1. Run: python main.py")
    print("2. Open: http://localhost:8000/static/calculation_ui.html")
    print("3. Create your first custom calculation!")

if __name__ == "__main__":
    main()