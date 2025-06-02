"""
Complete demonstration of the financial calculations system.
Run this after setting up the database to see the system in action.
"""

from app.core.database import SessionLocal, DWSessionLocal
from app.models.calculations import CalculationRepository
from app.services.calculation_builder import (
    DynamicSubqueryBuilder, CalculationManager, CalculationConfig,
    CalculationType, AggregationLevel
)
from app.models.dwh_models import Deal, Tranche, TrancheBal
import json

def demo_calculation_creation():
    """Demonstrate creating and saving calculations."""
    
    print("\n🎯 Demo: Creating Custom Calculations")
    print("=" * 50)
    
    session = SessionLocal()
    dw_session = DWSessionLocal()
    repo = CalculationRepository(session)
    
    # Create a deal-level sum calculation
    deal_total_config = CalculationConfig(
        name="deal_total_principal_dec2024",
        calculation_type=CalculationType.SUM,
        target_field="principal_distribution",
        aggregation_level=AggregationLevel.DEAL,
        cycle_filter=202412
    )
    
    saved_calc1 = repo.save_calculation(
        config=deal_total_config,
        name="Deal Total Principal Distribution (Dec 2024)",
        description="Sum of all principal distributions by deal for December 2024",
        created_by="demo_user",
        is_public=True
    )
    
    print(f"✓ Created: {saved_calc1.name}")
    
    # Create a weighted average calculation
    weighted_avg_config = CalculationConfig(
        name="weighted_avg_rate_dec2024",
        calculation_type=CalculationType.WEIGHTED_AVERAGE,
        target_field="pass_through_rate",
        weight_field="ending_balance",
        aggregation_level=AggregationLevel.DEAL,
        cycle_filter=202412
    )
    
    saved_calc2 = repo.save_calculation(
        config=weighted_avg_config,
        name="Weighted Average Pass-Through Rate (Dec 2024)",
        description="Balance-weighted average pass-through rate by deal",
        created_by="demo_user",
        is_public=False
    )
    
    print(f"✓ Created: {saved_calc2.name}")
    
    # Create a tranche-level percentage calculation
    percentage_config = CalculationConfig(
        name="principal_pct_of_balance",
        calculation_type=CalculationType.PERCENTAGE,
        target_field="principal_distribution",
        denominator_field="ending_balance",
        aggregation_level=AggregationLevel.TRANCHE,
        cycle_filter=202412
    )
    
    saved_calc3 = repo.save_calculation(
        config=percentage_config,
        name="Principal Distribution as % of Balance",
        description="Principal distribution as percentage of ending balance",
        created_by="demo_user",
        is_public=True
    )
    
    print(f"✓ Created: {saved_calc3.name}")
    
    session.close()
    dw_session.close()
    
    return [saved_calc1, saved_calc2, saved_calc3]

def demo_query_execution():
    """Demonstrate executing calculations in queries."""
    
    print("\n⚡ Demo: Executing Calculations in Queries")
    print("=" * 50)
    
    session = SessionLocal()
    dw_session = DWSessionLocal()
    repo = CalculationRepository(session)
    manager = CalculationManager(dw_session)
    
    # Get saved calculations
    saved_calcs = repo.get_user_calculations("demo_user")
    
    if not saved_calcs:
        print("❌ No saved calculations found. Run demo_calculation_creation() first.")
        session.close()
        dw_session.close()
        return
    
    # Convert to CalculationConfig objects (deal-level only for this demo)
    deal_configs = [
        calc.to_calculation_config() 
        for calc in saved_calcs 
        if calc.aggregation_level == "deal"
    ]
    
    if not deal_configs:
        print("❌ No deal-level calculations found.")
        session.close()
        dw_session.close()
        return
    
    print(f"Found {len(deal_configs)} deal-level calculations:")
    for config in deal_configs:
        print(f"  • {config.name} ({config.calculation_type.value})")
    
    # Create enhanced query
    try:
        enhanced_query = manager.create_enhanced_query("deal", deal_configs)
        
        print(f"\n🔍 Generated query with {len(deal_configs)} calculations")
        print("Query columns:")
        for desc in enhanced_query.column_descriptions:
            print(f"  • {desc['name']}")
        
        # Execute query
        results = enhanced_query.limit(5).all()
        print(f"\n📊 Query Results ({len(results)} deals):")
        print("-" * 40)
        
        for i, row in enumerate(results):
            print(f"\nDeal {i+1}:")
            
            # Handle different row types
            if hasattr(row, '_fields'):
                # SQLAlchemy Row object
                for field in row._fields:
                    value = getattr(row, field)
                    print(f"  {field}: {value}")
            else:
                # Tuple or other type
                for j, desc in enumerate(enhanced_query.column_descriptions):
                    col_name = desc['name']
                    try:
                        value = row[j] if hasattr(row, '__getitem__') else getattr(row, col_name, 'N/A')
                        if isinstance(value, float):
                            value = f"{value:,.2f}"
                        print(f"  {col_name}: {value}")
                    except (IndexError, AttributeError):
                        print(f"  {col_name}: N/A")
        
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        print("This might be due to missing sample data. Run setup_database.py first.")
    
    session.close()
    dw_session.close()

def demo_subquery_generation():
    """Demonstrate subquery generation."""
    
    print("\n🔧 Demo: Subquery Generation")
    print("=" * 50)
    
    dw_session = DWSessionLocal()
    builder = DynamicSubqueryBuilder(dw_session)
    
    # Create a sample configuration
    config = CalculationConfig(
        name="sample_calculation",
        calculation_type=CalculationType.SUM,
        target_field="principal_distribution",
        aggregation_level=AggregationLevel.DEAL,
        cycle_filter=202412
    )
    
    try:
        # Generate subquery
        subquery = builder.build_calculation_subquery(config)
        
        print("✓ Generated subquery successfully")
        print(f"Subquery columns: {list(subquery.c.keys())}")
        
        # Show SQL
        sql_str = str(subquery.compile(compile_kwargs={"literal_binds": True}))
        print(f"\nGenerated SQL:")
        print("-" * 20)
        print(sql_str)
        
    except Exception as e:
        print(f"❌ Subquery generation failed: {e}")
    
    dw_session.close()

def demo_search_and_templates():
    """Demonstrate search and template functionality."""
    
    print("\n🔍 Demo: Search and Templates")
    print("=" * 50)
    
    session = SessionLocal()
    repo = CalculationRepository(session)
    
    # Search calculations
    print("Searching for 'principal' calculations:")
    search_results = repo.search_calculations("principal", "demo_user")
    for result in search_results:
        print(f"  • {result.name} ({result.calculation_type})")
    
    print(f"\nFound {len(search_results)} matching calculations")
    
    # Get templates
    print("\nAvailable calculation templates:")
    templates = repo.get_calculation_templates()
    for template in templates:
        print(f"  • {template.name} ({template.category})")
        print(f"    {template.description}")
    
    print(f"\nFound {len(templates)} templates")
    
    session.close()

def demo_api_simulation():
    """Simulate API interactions."""
    
    print("\n🌐 Demo: API Simulation")
    print("=" * 50)
    
    # Simulate creating a calculation via API
    api_request = {
        "name": "API Test Calculation",
        "description": "Created via simulated API call",
        "calculation_type": "sum",
        "target_field": "interest_distribution",
        "aggregation_level": "deal",
        "cycle_filter": 202412,
        "is_public": False
    }
    
    print("Simulated API Request:")
    print(json.dumps(api_request, indent=2))
    
    # Process the request (like the API would)
    session = SessionLocal()
    repo = CalculationRepository(session)
    
    try:
        config = CalculationConfig(
            name=api_request["name"],
            calculation_type=CalculationType(api_request["calculation_type"]),
            target_field=api_request["target_field"],
            aggregation_level=AggregationLevel(api_request["aggregation_level"]),
            cycle_filter=api_request["cycle_filter"]
        )
        
        saved_calc = repo.save_calculation(
            config=config,
            name=api_request["name"],
            description=api_request["description"],
            created_by="api_user",
            is_public=api_request["is_public"]
        )
        
        print(f"\n✓ API calculation created: {saved_calc.name} (ID: {saved_calc.id})")
        
    except Exception as e:
        print(f"❌ API simulation failed: {e}")
    
    session.close()

def show_sample_data():
    """Show sample data in the data warehouse."""
    
    print("\n📊 Demo: Sample Data Overview")
    print("=" * 50)
    
    dw_session = DWSessionLocal()
    
    # Count records
    deal_count = dw_session.query(Deal).count()
    tranche_count = dw_session.query(Tranche).count()
    tranche_bal_count = dw_session.query(TrancheBal).count()
    
    print(f"Data Warehouse Summary:")
    print(f"  • Deals: {deal_count}")
    print(f"  • Tranches: {tranche_count}")
    print(f"  • Tranche Balance Records: {tranche_bal_count}")
    
    if deal_count > 0:
        print(f"\nSample Deal Data:")
        deals = dw_session.query(Deal).limit(3).all()
        for deal in deals:
            print(f"  • Deal {deal.dl_nbr}: {deal.issr_cde}")
    
    if tranche_bal_count > 0:
        print(f"\nSample Cycle Data:")
        cycles = dw_session.query(TrancheBal.cycle_cde).distinct().all()
        for cycle in cycles:
            count = dw_session.query(TrancheBal).filter(TrancheBal.cycle_cde == cycle[0]).count()
            print(f"  • Cycle {cycle[0]}: {count} records")
    
    dw_session.close()

def main():
    """Run the complete demonstration."""
    
    print("🚀 Financial Calculations System Demo")
    print("=" * 60)
    
    try:
        # Show sample data
        show_sample_data()
        
        # Demo core functionality
        demo_calculation_creation()
        demo_subquery_generation()
        demo_query_execution()
        demo_search_and_templates()
        demo_api_simulation()
        
        print("\n✅ Demo completed successfully!")
        print("\nNext Steps:")
        print("1. Start the API server: python main.py")
        print("2. Open the UI: http://localhost:8000/static/calculation_ui.html")
        print("3. Create calculations using the web interface")
        print("4. Use the saved calculations in your reports")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("Make sure you've run setup_database.py first")

if __name__ == "__main__":
    main()