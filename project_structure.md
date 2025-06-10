# Oracle Table Comparison Tool - Complete Project

## 📁 Project Directory Structure

```
oracle-table-compare/
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── table_comparator.py               # Main application script
├── config.yaml.sample               # Sample configuration file
├── config.yaml                      # Your actual configuration (create from sample)
├── setup.py                         # Optional: For pip installation
├── .gitignore                       # Git ignore file
├── docs/                            # Documentation
│   ├── USAGE.md                     # Detailed usage guide
│   ├── CONFIGURATION.md             # Configuration reference
│   └── TROUBLESHOOTING.md           # Common issues and solutions
├── scripts/                         # Utility scripts
│   ├── setup_environment.sh        # Environment setup script
│   ├── test_connection.py          # Database connection tester
│   └── cleanup_audit_tables.py     # Cleanup utility
├── tests/                           # Unit tests
│   ├── __init__.py
│   ├── test_comparator.py
│   ├── test_hash_generator.py
│   └── conftest.py
├── logs/                            # Log files (auto-created)
│   └── audit.log
├── output/                          # Generated SQL files (auto-created)
│   ├── source_sync_statements.sql
│   ├── target_sync_statements.sql
│   └── target_sync_statements_verified.sql
└── examples/                        # Example configurations
    ├── config_hr_employees.yaml
    ├── config_sales_orders.yaml
    └── batch_compare_multiple_tables.sh
```

## 🚀 Quick Setup Instructions

### 1. Create Project Directory
```bash
mkdir oracle-table-compare
cd oracle-table-compare
```

### 2. Download All Files
Copy all the files from the artifacts below into your project directory with the exact structure shown above.

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Your Environment
```bash
# Copy sample configuration
cp config.yaml.sample config.yaml

# Edit with your database details
vim config.yaml  # or use your preferred editor
```

### 5. Test Database Connectivity
```bash
python scripts/test_connection.py
```

### 6. Run Your First Comparison
```bash
python table_comparator.py config.yaml
```

## 📋 File Contents

Each file is provided in the artifacts below. Simply copy the content into files with the corresponding names in your project directory.
