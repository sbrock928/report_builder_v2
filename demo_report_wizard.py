"""
Comprehensive demonstration of the Report Wizard functionality.
Run this after setting up the database with setup_database.py.
"""

import requests
import json
import time
from datetime import datetime

def demo_report_wizard():
    """Demonstrate the complete Report Wizard functionality."""
    
    print("\n🧙‍♂️ Demo: Financial Report Wizard")
    print("=" * 60)
    
    base_url = "http://localhost:8000/api/report-wizard"
    
    # Test 1: Get wizard data
    print("\n🔍 Step 1: Loading Wizard Data")
    print("-" * 40)
    
    try:
        response = requests.get(f"{base_url}/wizard-data")
        if response.status_code == 200:
            wizard_data = response.json()
            print(f"✓ Loaded wizard data:")
            print(f"  • Available deals: {len(wizard_data['deals'])}")
            print(f"  • Available fields: {sum(len(fields) for fields in wizard_data['available_fields'].values())}")
            print(f"  • Filter operators: {len(wizard_data['filter_operators'])}")
            
            # Show sample data
            print(f"\n📋 Sample Deals:")
            for deal in wizard_data['deals'][:3]:
                print(f"  • {deal['dl_nbr']} - {deal['issr_cde']}")
                
            print(f"\n📊 Available Field Categories:")
            for scope, fields in wizard_data['available_fields'].items():
                categories = set(field['category'] for field in fields)
                print(f"  • {scope}: {', '.join(categories)}")
                
        else:
            print(f"❌ Failed to load wizard data: {response.status_code}")
            return
            
    except Exception as e:
        print(f"❌ Error loading wizard data: {e}")
        return
    
    # Test 2: Create a Deal-Level Report
    print("\n🏗️ Step 2: Creating Deal-Level Report")
    print("-" * 40)
    
    deal_report_config = {
        "name": "Deal Summary Report - Demo",
        "description": "Comprehensive deal-level financial summary created via API demo",
        "scope": "DEAL",
        "created_by": "demo_user",
        "selected_deals": [
            {"dl_nbr": 12341, "selected_tranches": []},
            {"dl_nbr": 12342, "selected_tranches": []},
            {"dl_nbr": 12343, "selected_tranches": []}
        ],
        "selected_fields": [
            {
                "field_name": "dl_nbr",
                "display_name": "Deal Number",
                "field_type": "number",
                "field_source": "raw_field",
                "is_required": True
            },
            {
                "field_name": "issr_cde",
                "display_name": "Issuer Code",
                "field_type": "text",
                "field_source": "raw_field",
                "is_required": True
            },
            {
                "field_name": "tr_end_bal_amt",
                "display_name": "Total Ending Balance",
                "field_type": "number",
                "field_source": "raw_field"
            },
            {
                "field_name": "tr_prin_dstrb_amt",
                "display_name": "Total Principal Distribution",
                "field_type": "number",
                "field_source": "raw_field"
            }
        ],
        "filter_conditions": []
    }
    
    try:
        response = requests.post(
            f"{base_url}/reports",
            json=deal_report_config
        )
        
        if response.status_code == 200:
            deal_report = response.json()
            print(f"✓ Created deal-level report: {deal_report['name']}")
            print(f"  • Report ID: {deal_report['id']}")
            print(f"  • Scope: {deal_report['scope']}")
            print(f"  • Selected deals: {len(deal_report['selected_deals'])}")
            print(f"  • Selected fields: {len(deal_report['selected_fields'])}")
            
            deal_report_id = deal_report['id']
        else:
            print(f"❌ Failed to create deal report: {response.status_code}")
            print(f"   Error: {response.text}")
            return
            
    except Exception as e:
        print(f"❌ Error creating deal report: {e}")
        return
    
    # Test 3: Create a Tranche-Level Report
    print("\n🔧 Step 3: Creating Tranche-Level Report")
    print("-" * 40)
    
    tranche_report_config = {
        "name": "Tranche Detail Report - Demo",
        "description": "Detailed tranche-level analysis with specific tranche selections",
        "scope": "TRANCHE",
        "created_by": "demo_user",
        "selected_deals": [
            {
                "dl_nbr": 12341, 
                "selected_tranches": [
                    {"dl_nbr": 12341, "tr_id": "A1"},
                    {"dl_nbr": 12341, "tr_id": "A2"}
                ]
            },
            {
                "dl_nbr": 12342,
                "selected_tranches": [
                    {"dl_nbr": 12342, "tr_id": "A1"}
                ]
            }
        ],
        "selected_fields": [
            {
                "field_name": "dl_nbr",
                "display_name": "Deal Number",
                "field_type": "number",
                "field_source": "raw_field",
                "is_required": True
            },
            {
                "field_name": "tr_id",
                "display_name": "Tranche ID",
                "field_type": "text",
                "field_source": "raw_field",
                "is_required": True
            },
            {
                "field_name": "tr_end_bal_amt",
                "display_name": "Ending Balance",
                "field_type": "number",
                "field_source": "raw_field"
            },
            {
                "field_name": "tr_pass_thru_rte",
                "display_name": "Pass Through Rate",
                "field_type": "percentage",
                "field_source": "raw_field"
            },
            {
                "field_name": "tr_prin_dstrb_amt",
                "display_name": "Principal Distribution",
                "field_type": "number",
                "field_source": "raw_field"
            }
        ],
        "filter_conditions": []
    }
    
    try:
        response = requests.post(
            f"{base_url}/reports",
            json=tranche_report_config
        )
        
        if response.status_code == 200:
            tranche_report = response.json()
            print(f"✓ Created tranche-level report: {tranche_report['name']}")
            print(f"  • Report ID: {tranche_report['id']}")
            print(f"  • Scope: {tranche_report['scope']}")
            print(f"  • Selected deals: {len(tranche_report['selected_deals'])}")
            print(f"  • Selected tranches: {sum(len(deal['selected_tranches']) for deal in tranche_report['selected_deals'])}")
            print(f"  • Selected fields: {len(tranche_report['selected_fields'])}")
            
            tranche_report_id = tranche_report['id']
        else:
            print(f"❌ Failed to create tranche report: {response.status_code}")
            print(f"   Error: {response.text}")
            return
            
    except Exception as e:
        print(f"❌ Error creating tranche report: {e}")
        return
    
    # Test 4: Execute Deal-Level Report
    print("\n⚡ Step 4: Executing Deal-Level Report")
    print("-" * 40)
    
    try:
        print("Executing deal-level report... (this may take a moment)")
        start_time = time.time()
        
        response = requests.post(
            f"{base_url}/reports/{deal_report_id}/execute?cycle_filter=202412"
        )
        
        execution_time = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Deal report executed successfully!")
            print(f"  • Execution time: {execution_time:.2f} seconds")
            print(f"  • API execution time: {result['execution_time_ms']}ms")
            print(f"  • Columns: {len(result['columns'])}")
            print(f"  • Rows: {result['row_count']}")
            print(f"  • Cycle filter: {result['cycle_filter']}")
            
            # Show column names
            print(f"\n📋 Report Columns:")
            for col in result['columns']:
                print(f"  • {col['header']} ({col['type']})")
            
            # Show sample data
            if result['rows']:
                print(f"\n📊 Sample Data (first 2 rows):")
                for i, row in enumerate(result['rows'][:2]):
                    print(f"  Row {i+1}:")
                    for col in result['columns']:
                        value = row.get(col['field'], 'N/A')
                        if isinstance(value, float):
                            value = f"{value:,.2f}"
                        print(f"    {col['header']}: {value}")
                    print()
                    
        else:
            print(f"❌ Failed to execute deal report: {response.status_code}")
            print(f"   Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Error executing deal report: {e}")
    
    # Test 5: Execute Tranche-Level Report
    print("\n🔍 Step 5: Executing Tranche-Level Report")
    print("-" * 40)
    
    try:
        print("Executing tranche-level report...")
        start_time = time.time()
        
        response = requests.post(
            f"{base_url}/reports/{tranche_report_id}/execute?cycle_filter=202412"
        )
        
        execution_time = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Tranche report executed successfully!")
            print(f"  • Execution time: {execution_time:.2f} seconds")
            print(f"  • API execution time: {result['execution_time_ms']}ms")
            print(f"  • Rows: {result['row_count']}")
            
            # Show sample data
            if result['rows']:
                print(f"\n📊 Sample Tranche Data:")
                for i, row in enumerate(result['rows'][:3]):
                    deal_num = row.get('dl_nbr', 'N/A')
                    tranche_id = row.get('tr_id', 'N/A')
                    balance = row.get('tr_end_bal_amt', 0)
                    rate = row.get('tr_pass_thru_rte', 0)
                    
                    print(f"  • Deal {deal_num}, Tranche {tranche_id}: ${balance:,.0f} @ {rate*100:.2f}%")
                    
        else:
            print(f"❌ Failed to execute tranche report: {response.status_code}")
            print(f"   Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Error executing tranche report: {e}")
    
    # Test 6: Export Reports
    print("\n📄 Step 6: Testing Export Functionality")
    print("-" * 40)
    
    # Export deal report as CSV
    try:
        response = requests.post(
            f"{base_url}/reports/{deal_report_id}/export/csv?cycle_filter=202412"
        )
        
        if response.status_code == 200:
            # Save the CSV file
            filename = "demo_deal_report.csv"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✓ Deal report exported as CSV!")
            print(f"  • Filename: {filename}")
            print(f"  • File size: {file_size:,} bytes")
            
        else:
            print(f"❌ Failed to export deal report CSV: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error exporting deal report CSV: {e}")
    
    # Export tranche report as Excel
    try:
        response = requests.post(
            f"{base_url}/reports/{tranche_report_id}/export/excel?cycle_filter=202412"
        )
        
        if response.status_code == 200:
            # Save the Excel file
            filename = "demo_tranche_report.xlsx"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✓ Tranche report exported as Excel!")
            print(f"  • Filename: {filename}")
            print(f"  • File size: {file_size:,} bytes")
            
        else:
            print(f"❌ Failed to export tranche report Excel: {response.status_code}")
            print(f"   Note: Make sure openpyxl is installed: pip install openpyxl")
            
    except Exception as e:
        print(f"❌ Error exporting tranche report Excel: {e}")
    
    # Test 7: List All Reports
    print("\n📋 Step 7: Listing All Reports")
    print("-" * 40)
    
    try:
        response = requests.get(f"{base_url}/reports")
        
        if response.status_code == 200:
            reports = response.json()
            print(f"✓ Found {len(reports)} total reports:")
            
            for report in reports:
                created_date = datetime.fromisoformat(report['created_date'].replace('Z', '+00:00'))
                print(f"  • {report['name']}")
                print(f"    - ID: {report['id']}, Scope: {report['scope']}")
                print(f"    - Deals: {report['deal_count']}, Fields: {report['field_count']}")
                print(f"    - Created: {created_date.strftime('%Y-%m-%d %H:%M')}")
                print()
                
        else:
            print(f"❌ Failed to list reports: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error listing reports: {e}")

def demo_wizard_ui_integration():
    """Demonstrate UI integration points."""
    
    print("\n🌐 Demo: Wizard UI Integration")
    print("=" * 40)
    
    print("✅ Report Wizard is now available at:")
    print("   http://localhost:8000/report-wizard")
    print("\n🧙‍♂️ Features available in the Wizard:")
    print("   • Step-by-step report creation")
    print("   • Deal and tranche selection with search/filtering")
    print("   • Raw field and saved calculation integration")
    print("   • Real-time report execution with cycle filtering")
    print("   • CSV and Excel export capabilities")
    print("   • Report management (save, edit, duplicate, delete)")
    
    print("\n🔗 Integration with existing system:")
    print("   • All saved calculations from Calculation Builder")
    print("   • Are automatically available as fields in Report Wizard")
    print("   • Reports can combine raw fields + saved calculations")
    print("   • Maintains backward compatibility with existing data")
    
    print("\n🎯 Advanced capabilities:")
    print("   • Deal-level reports aggregate tranche data automatically")
    print("   • Tranche-level reports show detailed individual records")
    print("   • Cycle filtering applied at execution time (not saved)")
    print("   • Future: Filter conditions, scheduling, sharing")

def demo_api_endpoints():
    """Demonstrate all available API endpoints."""
    
    print("\n🔌 Demo: Available API Endpoints")
    print("=" * 40)
    
    endpoints = [
        ("GET", "/wizard-data", "Get all wizard configuration data"),
        ("GET", "/deals", "Get available deals with search/pagination"),
        ("POST", "/tranches", "Get tranches for specific deals"),
        ("GET", "/fields/{scope}", "Get available fields by scope"),
        ("POST", "/reports", "Create new report configuration"),
        ("GET", "/reports", "List all report configurations"),
        ("GET", "/reports/{id}", "Get specific report configuration"),
        ("PUT", "/reports/{id}", "Update existing report configuration"),
        ("DELETE", "/reports/{id}", "Delete report configuration"),
        ("GET", "/reports/{id}/schema", "Get report schema for preview"),
        ("POST", "/reports/{id}/execute", "Execute report with cycle filter"),
        ("POST", "/reports/{id}/export/csv", "Export report as CSV"),
        ("POST", "/reports/{id}/export/excel", "Export report as Excel")
    ]
    
    print("📚 Report Wizard API Endpoints:")
    print("   Base URL: http://localhost:8000/api/report-wizard")
    print()
    
    for method, endpoint, description in endpoints:
        print(f"   {method:6} {endpoint:25} - {description}")
    
    print("\n📖 Documentation:")
    print("   • Full API docs: http://localhost:8000/docs")
    print("   • Interactive testing available in Swagger UI")

def main():
    """Run the complete Report Wizard demonstration."""
    
    print("🚀 Financial Report Wizard Demo")
    print("=" * 60)
    
    # Check if server is running
    try:
        response = requests.get("http://localhost:8000/health", timeout=3)
        if response.status_code != 200:
            print("❌ Server is not responding correctly")
            return
    except Exception:
        print("❌ Server is not running. Start it with: python main.py")
        return
    
    try:
        # Run the main demo
        demo_report_wizard()
        demo_wizard_ui_integration()
        demo_api_endpoints()
        
        print("\n✅ Report Wizard Demo completed successfully!")
        print("\n🎯 Next Steps:")
        print("1. Open the Report Wizard UI: http://localhost:8000/report-wizard")
        print("2. Create custom reports using the step-by-step wizard")
        print("3. Combine raw fields with your saved calculations")
        print("4. Export reports for analysis in Excel or other tools")
        print("5. Integrate with your BI/reporting workflows via API")
        
        print("\n🔄 Integration with existing tools:")
        print("• Calculation Builder: http://localhost:8000/calculation-builder")
        print("• Legacy Report Builder: http://localhost:8000/report-builder")
        print("• API Documentation: http://localhost:8000/docs")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("Make sure you've run setup_database.py first")

if __name__ == "__main__":
    main()