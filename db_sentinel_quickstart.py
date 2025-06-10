#!/usr/bin/env python3
"""
DB-Sentinel Quick Start Example
==============================

This example demonstrates how to use DB-Sentinel for different common scenarios:
1. Auto-discovery of tables
2. Manual configuration creation
3. Running comparisons
4. Analyzing results

Run this script to see DB-Sentinel in action with sample configurations.
"""

import os
import sys
import yaml
import tempfile
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

def create_sample_config():
    """Create a sample configuration file for demonstration."""
    
    config = {
        'source_db': {
            'user': 'hr_source',
            'password': 'source_password',
            'dsn': 'source-db:1521/ORCL',
            'connection_timeout': 60,
            'query_timeout': 900
        },
        'target_db': {
            'user': 'hr_target', 
            'password': 'target_password',
            'dsn': 'target-db:1521/ORCL',
            'connection_timeout': 60,
            'query_timeout': 900
        },
        'global_config': {
            'schema': 'HR',
            'max_threads': 6,
            'batch_timeout': 600,
            'enable_restart': True,
            'enable_reverification': True,
            'enable_data_masking': False,
            'output_directory': './output',
            'log_directory': './logs',
            'archive_directory': './archive'
        },
        'tables': [
            # Example 1: Simple employee table
            {
                'table_name': 'EMPLOYEES',
                'primary_key': ['EMPLOYEE_ID'],
                'chunk_size': 10000,
                '_description': 'Main employee master data'
            },
            
            # Example 2: Order items with composite key
            {
                'table_name': 'ORDER_ITEMS',
                'primary_key': ['ORDER_ID', 'ITEM_ID'],
                'chunk_size': 5000,
                'columns': ['ORDER_ID', 'ITEM_ID', 'QUANTITY', 'PRICE', 'STATUS'],
                '_description': 'Order line items with composite primary key'
            },
            
            # Example 3: Recent transactions only
            {
                'table_name': 'TRANSACTIONS',
                'primary_key': ['TRANSACTION_ID'],
                'chunk_size': 15000,
                'where_clause': "TRANSACTION_DATE >= DATE '2024-01-01'",
                '_description': 'Recent transactions (2024 onwards)'
            },
            
            # Example 4: Large audit table with optimization
            {
                'table_name': 'AUDIT_LOG',
                'primary_key': ['LOG_ID'],
                'chunk_size': 2000,
                'max_threads': 8,
                'columns': ['LOG_ID', 'USER_ID', 'ACTION', 'TIMESTAMP', 'STATUS'],
                'where_clause': "STATUS = 'ACTIVE'",
                '_description': 'Active audit records with smaller chunks'
            },
            
            # Example 5: Customer data in different schema
            {
                'table_name': 'CUSTOMER_DATA',
                'schema': 'SALES',
                'primary_key': ['CUSTOMER_ID'],
                'chunk_size': 8000,
                'max_threads': 4,
                'columns': ['CUSTOMER_ID', 'NAME', 'EMAIL', 'PHONE', 'STATUS'],
                'where_clause': "CREATED_DATE >= TRUNC(SYSDATE) - 30",
                '_description': 'Customer data from last 30 days'
            }
        ]
    }
    
    return config


def demonstrate_discovery():
    """Demonstrate the table discovery functionality."""
    print("🔍 DB-Sentinel Table Discovery Example")
    print("=" * 50)
    
    print("""
To discover tables in your database and auto-generate configuration:

    python scripts/db_sentinel_utils.py discover \\
        --user your_username \\
        --password your_password \\
        --dsn your_host:1521/YOUR_DB \\
        --schema YOUR_SCHEMA \\
        --output discovered_config.yaml

This will:
✅ Connect to your database
✅ Analyze all tables in the schema
✅ Detect primary keys automatically
✅ Recommend optimal chunk sizes
✅ Generate performance recommendations
✅ Create a ready-to-use configuration file
    """)


def demonstrate_manual_config():
    """Demonstrate manual configuration creation."""
    print("\n📝 Manual Configuration Example")
    print("=" * 40)
    
    # Create sample config
    config = create_sample_config()
    
    # Write to temporary file for demonstration
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f, default_flow_style=False, indent=2, sort_keys=False)
        config_file = f.name
    
    print(f"✅ Sample configuration created: {config_file}")
    print(f"📊 Tables configured: {len(config['tables'])}")
    
    print("\nConfiguration breakdown:")
    for i, table in enumerate(config['tables'], 1):
        table_name = table['table_name']
        schema = table.get('schema', config['global_config']['schema'])
        chunk_size = table['chunk_size']
        pk_count = len(table['primary_key'])
        
        filters = []
        if table.get('columns'):
            filters.append(f"columns: {len(table['columns'])}")
        if table.get('where_clause'):
            filters.append("filtered")
        if table.get('max_threads'):
            filters.append(f"threads: {table['max_threads']}")
        
        filter_str = f" ({', '.join(filters)})" if filters else ""
        
        print(f"  {i}. {schema}.{table_name}")
        print(f"     └─ chunk_size: {chunk_size:,}, primary_keys: {pk_count}{filter_str}")
    
    return config_file


def demonstrate_validation():
    """Demonstrate configuration validation."""
    print("\n✅ Configuration Validation Example")
    print("=" * 42)
    
    print("""
To validate your configuration before running:

    python scripts/db_sentinel_utils.py validate config.yaml

The validator checks:
✓ Required configuration sections
✓ Database connection parameters
✓ Table configuration validity
✓ Primary key specifications
✓ Performance settings
✓ Memory usage estimates

Example validation output:
    ✅ Configuration is valid!
    ⚠️  Validation Warnings:
      • tables[3] (AUDIT_LOG).chunk_size should be between 100 and 100,000
      • High memory usage estimated (8,192.5 MB). Consider reducing chunk_size.
    """)


def demonstrate_execution():
    """Demonstrate script execution."""
    print("\n🚀 Running DB-Sentinel")
    print("=" * 25)
    
    print("""
To execute the comparison:

    python db_sentinel.py config.yaml

DB-Sentinel will:
1️⃣  Validate configuration
2️⃣  Connect to source and target databases  
3️⃣  Analyze each table individually
4️⃣  Process data in parallel batches
5️⃣  Generate comparison statistics
6️⃣  Create synchronization SQL scripts
7️⃣  Produce comprehensive reports

Example output:
    🚀 Starting DB-Sentinel...
    ============================================================
    ✅ Table configuration valid: HR.EMPLOYEES
    ✅ Table configuration valid: HR.ORDER_ITEMS
    ✅ Table configuration valid: HR.TRANSACTIONS
    
    Processing EMPLOYEES: 100%|████████| 12/12 [00:15<00:00,  1.25s/batch]
    Processing ORDER_ITEMS: 100%|███████| 547/547 [02:18<00:00,  3.95batch/s]
    
    🎉 DB-Sentinel completed successfully!
    """)


def demonstrate_results():
    """Demonstrate result analysis."""
    print("\n📊 Results and Reporting")
    print("=" * 28)
    
    print("""
After completion, DB-Sentinel generates:

📄 Summary Report (summary_report_dbsentinel_YYYYMMDD_HHMMSS.txt):
    ================================================================================
                                DB-SENTINEL COMPARISON REPORT
    ================================================================================
    Job ID: dbsentinel_20241209_143022
    Duration: 1,247.32 seconds
    
    SUMMARY:
    --------
    Total Tables: 5
    Successful: 5
    Failed: 0
    Success Rate: 100.0%
    
    DATA STATISTICS:
    ---------------
    Total Source Rows: 2,847,291
    Total Target Rows: 2,847,336
    Total Mismatches: 45
    Data Accuracy: 99.998%

📄 SQL Sync Scripts:
    • source_sync_statements.sql    - Scripts to update source DB
    • target_sync_statements.sql    - Scripts to update target DB

📄 Detailed Logs:
    • db_sentinel_YYYYMMDD.log      - Application execution logs
    • audit_trail.log               - Audit information

📊 Performance Metrics:
    • Processing rate: ~2,284 rows/second
    • Memory efficiency: 6.2 GB peak usage
    • Thread utilization: 85% average
    """)


def show_advanced_examples():
    """Show advanced configuration examples."""
    print("\n🔧 Advanced Configuration Examples")
    print("=" * 40)
    
    examples = {
        "High-Performance Large Table": {
            'table_name': 'SALES_HISTORY',
            'schema': 'ANALYTICS', 
            'primary_key': ['SALE_ID'],
            'chunk_size': 50000,
            'max_threads': 16,
            'columns': ['SALE_ID', 'SALE_DATE', 'CUSTOMER_ID', 'AMOUNT'],
            'where_clause': 'SALE_DATE >= ADD_MONTHS(SYSDATE, -12)'
        },
        
        "Reference Data (Small & Fast)": {
            'table_name': 'LOOKUP_CODES',
            'primary_key': ['CODE_TYPE', 'CODE_VALUE'],
            'chunk_size': 500,
            'max_threads': 2
        },
        
        "Financial Data (Current Month)": {
            'table_name': 'ACCOUNT_BALANCES',
            'schema': 'FINANCE',
            'primary_key': ['ACCOUNT_ID', 'BALANCE_DATE'],
            'chunk_size': 20000,
            'columns': ['ACCOUNT_ID', 'BALANCE_DATE', 'BALANCE_AMOUNT', 'CURRENCY'],
            'where_clause': "BALANCE_DATE >= TRUNC(SYSDATE, 'MM')"
        },
        
        "User Activity (Last 7 Days)": {
            'table_name': 'USER_SESSIONS',
            'schema': 'APPLICATION',
            'primary_key': ['SESSION_ID'],
            'chunk_size': 12000,
            'max_threads': 6,
            'columns': ['SESSION_ID', 'USER_ID', 'LOGIN_TIME', 'IP_ADDRESS', 'STATUS'],
            'where_clause': "LOGIN_TIME >= SYSDATE - INTERVAL '7' DAY AND STATUS != 'INVALID'"
        }
    }
    
    for name, config in examples.items():
        print(f"\n💡 {name}:")
        
        # Format configuration nicely
        yaml_str = yaml.dump([config], default_flow_style=False, indent=2)
        # Indent each line
        indented_yaml = '\n'.join('    ' + line for line in yaml_str.split('\n'))
        print(indented_yaml)


def show_performance_tips():
    """Show performance optimization tips."""
    print("\n⚡ Performance Optimization Tips")
    print("=" * 35)
    
    tips = [
        ("Chunk Size Tuning", "Start with 10K, increase for large tables (up to 50K), decrease for wide rows"),
        ("Thread Optimization", "Use 1 thread per CPU core, reduce for high-latency connections"),
        ("Memory Management", "Keep total memory under 8GB: chunk_size × max_threads × row_size × 3"),
        ("Database Indexes", "Ensure primary key columns have indexes for fast sorting"),
        ("Network Location", "Run DB-Sentinel close to databases to minimize network latency"),
        ("WHERE Clauses", "Use date filters to compare recent data first, full history later"),
        ("Column Selection", "For wide tables, specify only necessary columns to reduce I/O"),
        ("Connection Pooling", "Increase connection_timeout for busy databases"),
    ]
    
    for i, (tip_name, tip_desc) in enumerate(tips, 1):
        print(f"{i:2d}. {tip_name}:")
        print(f"    {tip_desc}")


def main():
    """Main demonstration function."""
    print("🛡️  DB-SENTINEL QUICK START GUIDE")
    print("=" * 60)
    print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run demonstrations
    demonstrate_discovery()
    config_file = demonstrate_manual_config()
    demonstrate_validation()
    demonstrate_execution()
    demonstrate_results()
    show_advanced_examples()
    show_performance_tips()
    
    print("\n🎯 Next Steps")
    print("=" * 15)
    print("""
1. Install dependencies:
   pip install -r requirements.txt

2. Discover your tables:
   python scripts/db_sentinel_utils.py discover --user YOUR_USER --password YOUR_PASS --dsn YOUR_DSN --schema YOUR_SCHEMA

3. Validate configuration:
   python scripts/db_sentinel_utils.py validate config.yaml

4. Run your first comparison:
   python db_sentinel.py config.yaml

5. Review results and optimize:
   - Check summary report
   - Adjust chunk sizes and thread counts
   - Add WHERE clauses for filtering
   - Select specific columns for wide tables

📚 For detailed documentation, see README_DB-Sentinel.md
🆘 For support, visit: https://github.com/yourorg/db-sentinel/issues
    """)
    
    # Cleanup temporary file
    try:
        os.unlink(config_file)
        print(f"\n🧹 Cleaned up temporary file: {config_file}")
    except:
        pass


if __name__ == "__main__":
    main()
