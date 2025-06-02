"""
Demonstration of the Report Builder functionality.
Run this after setting up the database and running demo.py.
"""

import requests
import json
import time

def demo_report_builder():
    """Demonstrate creating and exporting financial reports."""
    
    print("\n📊 Demo: Financial Report Builder")
    print("=" * 60)
    
    base_url = "http://localhost:8000/api"
    
    # Test 1: Get available calculations
    print("\n🔍 Step 1: Loading Available Calculations")
    print("-" * 40)
    
    try:
        response = requests.get(f"{base_url}/reports/available-calculations")
        if response.status_code == 200:
            calculations = response.json()
            print(f"✓ Found {len(calculations)} available calculations:")
            
            for calc in calculations[:5]:  # Show first 5
                print(f"  • {calc['name']} ({calc['calculation_type']}, {calc['aggregation_level']})")
            
            if len(calculations) > 5:
                print(f"  ... and {len(calculations) - 5} more")
                
        else:
            print(f"❌ Failed to load calculations: {response.status_code}")
            return
            
    except Exception as e:
        print(f"❌ Error loading calculations: {e}")
        return
    
    # Test 2: Preview SQL for a combined report
    print("\n🔧 Step 2: Generating SQL Preview")
    print("-" * 40)
    
    # Select deal-level calculations for the report
    deal_calcs = [calc for calc in calculations if calc['aggregation_level'] == 'deal']
    
    if len(deal_calcs) < 2:
        print("❌ Need at least 2 deal-level calculations for demo")
        return
    
    selected_calc_ids = [calc['id'] for calc in deal_calcs[:3]]  # Use first 3
    
    preview_request = {
        "calculation_ids": selected_calc_ids,
        "base_aggregation": "deal",
        "cycle_filter": 202412
    }
    
    try:
        response = requests.post(
            f"{base_url}/reports/preview-sql",
            json=preview_request
        )
        
        if response.status_code == 200:
            preview = response.json()
            print(f"✓ Generated SQL for {preview['calculation_count']} calculations")
            print("SQL Preview (truncated):")
            sql_preview = preview['sql_query'][:300] + "..." if len(preview['sql_query']) > 300 else preview['sql_query']
            print(f"  {sql_preview}")
        else:
            print(f"❌ Failed to generate SQL preview: {response.status_code}")
            print(f"   Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Error generating SQL preview: {e}")
    
    # Test 3: Execute the report
    print("\n⚡ Step 3: Executing Report")
    print("-" * 40)
    
    report_request = {
        "calculation_ids": selected_calc_ids,
        "base_aggregation": "deal",
        "cycle_filter": 202412,
        "report_name": "Demo Financial Report"
    }
    
    try:
        print("Executing report... (this may take a moment)")
        start_time = time.time()
        
        response = requests.post(
            f"{base_url}/reports/execute",
            json=report_request
        )
        
        execution_time = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Report executed successfully!")
            print(f"  • Execution time: {execution_time:.2f} seconds")
            print(f"  • Columns: {len(result['columns'])}")
            print(f"  • Rows: {result['row_count']}")
            print(f"  • Column names: {', '.join(result['columns'][:5])}{'...' if len(result['columns']) > 5 else ''}")
            
            # Show sample data
            if result['rows']:
                print("\n📋 Sample Data (first 3 rows):")
                for i, row in enumerate(result['rows'][:3]):
                    formatted_row = []
                    for j, cell in enumerate(row):
                        if isinstance(cell, float):
                            formatted_row.append(f"{cell:,.2f}")
                        else:
                            formatted_row.append(str(cell) if cell is not None else "")
                    print(f"  Row {i+1}: {' | '.join(formatted_row[:4])}...")
                    
        else:
            print(f"❌ Failed to execute report: {response.status_code}")
            print(f"   Error: {response.text}")
            return
            
    except Exception as e:
        print(f"❌ Error executing report: {e}")
        return
    
    # Test 4: Export as CSV
    print("\n📄 Step 4: Exporting Report as CSV")
    print("-" * 40)
    
    try:
        response = requests.post(
            f"{base_url}/reports/export/csv",
            json=report_request
        )
        
        if response.status_code == 200:
            # Save the CSV file
            filename = "demo_financial_report.csv"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✓ CSV exported successfully!")
            print(f"  • Filename: {filename}")
            print(f"  • File size: {file_size:,} bytes")
            
        else:
            print(f"❌ Failed to export CSV: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error exporting CSV: {e}")
    
    # Test 5: Export as Excel (if openpyxl is available)
    print("\n📊 Step 5: Exporting Report as Excel")
    print("-" * 40)
    
    try:
        response = requests.post(
            f"{base_url}/reports/export/excel",
            json=report_request
        )
        
        if response.status_code == 200:
            # Save the Excel file
            filename = "demo_financial_report.xlsx"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✓ Excel exported successfully!")
            print(f"  • Filename: {filename}")
            print(f"  • File size: {file_size:,} bytes")
            
        else:
            print(f"❌ Failed to export Excel: {response.status_code}")
            print(f"   Note: Make sure openpyxl is installed: pip install openpyxl")
            
    except Exception as e:
        print(f"❌ Error exporting Excel: {e}")
    
    # Test 6: Demonstrate filtering capabilities
    print("\n🔎 Step 6: Demonstrating Advanced Features")
    print("-" * 40)
    
    # Try with different cycle filter
    advanced_request = {
        "calculation_ids": selected_calc_ids,
        "base_aggregation": "deal", 
        "cycle_filter": 202411,  # Different cycle
        "report_name": "November 2024 Report"
    }
    
    try:
        response = requests.post(
            f"{base_url}/reports/execute",
            json=advanced_request
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Advanced report (Nov 2024 cycle):")
            print(f"  • Rows found: {result['row_count']}")
            print(f"  • Execution time: {result['execution_time_ms']}ms")
        else:
            print(f"❌ Advanced report failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error with advanced report: {e}")

def demo_report_ui_integration():
    """Demonstrate UI integration points."""
    
    print("\n🌐 Demo: UI Integration")
    print("=" * 40)
    
    print("✅ Report Builder is now available at:")
    print("   http://localhost:8000/report-builder")
    print("\n📋 Features available in the UI:")
    print("   • Select multiple saved calculations")
    print("   • Filter by calculation type and aggregation level")
    print("   • Preview generated SQL queries")
    print("   • Execute reports with real-time results")
    print("   • Export to CSV and Excel formats")
    print("   • Apply cycle filters and custom naming")
    
    print("\n🔗 Integration with Calculation Builder:")
    print("   • All calculations created in the Calculation Builder")
    print("   • Are automatically available in the Report Builder")
    print("   • Public calculations can be used by all users")
    print("   • Private calculations only by their creators")

def main():
    """Run the complete Report Builder demonstration."""
    
    print("🚀 Financial Report Builder Demo")
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
        demo_report_builder()
        demo_report_ui_integration()
        
        print("\n✅ Report Builder Demo completed successfully!")
        print("\n🎯 Next Steps:")
        print("1. Open the Report Builder UI: http://localhost:8000/report-builder")
        print("2. Create custom reports by selecting calculations")
        print("3. Export reports for analysis in Excel or other tools")
        print("4. Integrate with your existing BI/reporting workflows")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("Make sure you've run setup_database.py and demo.py first")

if __name__ == "__main__":
    main()