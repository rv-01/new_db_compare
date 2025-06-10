# 🛡️ DB-Sentinel: Enterprise Database Comparison Tool

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Enterprise Ready](https://img.shields.io/badge/Enterprise-Ready-green.svg)](https://github.com/yourorg/db-sentinel)

**DB-Sentinel** is a production-ready enterprise database comparison tool that supports flexible multi-table configurations, advanced performance optimization, and comprehensive monitoring.

## ✨ Key Features

### 🎯 **Multi-Table Support**
- **Flexible Configuration**: Compare multiple tables with individual settings
- **Composite Primary Keys**: Support for complex primary key combinations
- **Column-Level Control**: Select specific columns or compare all
- **WHERE Clause Filtering**: Apply business logic filters per table

### 🚀 **Enterprise Performance**
- **Intelligent Chunking**: Configurable chunk sizes per table
- **Multi-threaded Processing**: Parallel execution with thread control
- **Memory Optimization**: Efficient memory usage for large datasets
- **Connection Pooling**: Optimized database connections

### 🔍 **Advanced Comparison**
- **Row-Level Hashing**: MD5/SHA256 hashing for accurate detection
- **Differential Analysis**: Identifies inserts, updates, and deletes
- **Data Integrity**: Preserves referential relationships
- **Resume Capability**: Checkpoint-based restart for long operations

### 📊 **Comprehensive Reporting**
- **Detailed Statistics**: Per-table and aggregate metrics
- **SQL Generation**: Automated sync script creation
- **Progress Tracking**: Real-time progress with tqdm
- **Audit Trails**: Complete operation logging

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐
│   Source DB     │    │   Target DB     │
│                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │  Table A    │◄┼────┼►│  Table A    │ │
│ │  Table B    │ │    │ │  Table B    │ │
│ │  Table C    │ │    │ │  Table C    │ │
│ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘
         │                       │
         └───────────┬───────────┘
                     │
         ┌───────────▼───────────┐
         │     DB-Sentinel       │
         │   Multi-Table Engine  │
         │                       │
         │ ├ Table A: 4 threads  │
         │ ├ Table B: 6 threads  │
         │ ├ Table C: 2 threads  │
         │ └ Custom configs      │
         └───────────┬───────────┘
                     │
         ┌───────────▼───────────┐
         │       Output          │
         │                       │
         │ • Comparison Report   │
         │ • Sync SQL Scripts    │
         │ • Audit Logs          │
         │ • Performance Metrics │
         └───────────────────────┘
```

## 🚀 Quick Start

### 1. **Installation**
```bash
git clone https://github.com/yourorg/db-sentinel.git
cd db-sentinel
pip install -r requirements.txt
```

### 2. **Discover Your Tables**
```bash
# Auto-discover tables and generate configuration
python scripts/db_sentinel_utils.py discover \
    --user hr_user \
    --password hr_pass \
    --dsn oracle-host:1521/ORCL \
    --schema HR \
    --output my_config.yaml
```

### 3. **Validate Configuration**
```bash
# Validate your configuration
python scripts/db_sentinel_utils.py validate my_config.yaml
```

### 4. **Run Comparison**
```bash
# Execute comparison
python db_sentinel.py my_config.yaml
```

## ⚙️ Configuration Examples

### **Multi-Table Configuration**
```yaml
# Global settings
global_config:
  schema: HR
  max_threads: 6
  enable_restart: true

# Individual table configurations
tables:
  # Simple table - all columns
  - table_name: "EMPLOYEES"
    primary_key: ["EMPLOYEE_ID"]
    chunk_size: 10000

  # Composite primary key
  - table_name: "ORDER_ITEMS"
    primary_key: ["ORDER_ID", "ITEM_ID"]
    chunk_size: 5000
    columns: ["ORDER_ID", "ITEM_ID", "QUANTITY", "PRICE", "STATUS"]

  # Filtered data with WHERE clause
  - table_name: "TRANSACTIONS"
    primary_key: ["TRANSACTION_ID"]
    chunk_size: 15000
    where_clause: "TRANSACTION_DATE >= DATE '2024-01-01'"

  # Custom schema and threading
  - table_name: "CUSTOMER_DATA"
    schema: "SALES"
    primary_key: ["CUSTOMER_ID"]
    chunk_size: 8000
    max_threads: 8
    where_clause: "STATUS = 'ACTIVE'"
```

### **Performance Tuning Examples**

#### Large Table (10M+ rows)
```yaml
- table_name: "SALES_HISTORY"
  primary_key: ["SALE_ID"]
  chunk_size: 25000        # Larger chunks
  max_threads: 10          # More parallelism
  columns: ["SALE_ID", "SALE_DATE", "AMOUNT", "STATUS"]  # Key columns only
  where_clause: "SALE_DATE >= ADD_MONTHS(SYSDATE, -12)"  # Recent data
```

#### Small Reference Table
```yaml
- table_name: "LOOKUP_CODES"
  primary_key: ["CODE_TYPE", "CODE_VALUE"]
  chunk_size: 1000         # Smaller chunks
  max_threads: 2           # Limited parallelism
```

#### Financial Data
```yaml
- table_name: "ACCOUNT_BALANCES"
  schema: "FINANCE"
  primary_key: ["ACCOUNT_ID", "BALANCE_DATE"]
  chunk_size: 20000
  columns: ["ACCOUNT_ID", "BALANCE_DATE", "BALANCE_AMOUNT", "CURRENCY"]
  where_clause: "BALANCE_DATE >= TRUNC(SYSDATE, 'MM')"  # Current month
```

## 🛠️ Utility Commands

### **Table Discovery**
```bash
# Discover all tables in schema
python scripts/db_sentinel_utils.py discover \
    --user myuser --password mypass --dsn host:1521/db \
    --schema SALES

# Discover specific tables
python scripts/db_sentinel_utils.py discover \
    --user myuser --password mypass --dsn host:1521/db \
    --schema HR \
    --tables EMPLOYEES DEPARTMENTS JOBS

# Discover by pattern
python scripts/db_sentinel_utils.py discover \
    --user myuser --password mypass --dsn host:1521/db \
    --schema ANALYTICS \
    --patterns "FACT_%%" "DIM_%%"
```

### **Configuration Validation**
```bash
# Validate configuration file
python scripts/db_sentinel_utils.py validate config.yaml

# Example output:
# ✅ Configuration is valid!
# ⚠️  Validation Warnings:
#   • tables[2] (BIG_TABLE).chunk_size should be between 100 and 100,000
```

### **Schema Analysis**
```bash
# Analyze database schema
python scripts/db_sentinel_utils.py analyze \
    --user myuser --password mypass --dsn host:1521/db \
    --schema SALES

# Example output:
# 📋 Schema Analysis Results:
# Schema: SALES
# Total tables: 25
# Small tables (< 100K rows): 15
# Medium tables (100K - 1M rows): 8  
# Large tables (> 1M rows): 2
```

## 📊 Output and Reporting

### **Generated Files**
```
output/
├── summary_report_dbsentinel_20241209_143022.txt    # Executive summary
├── source_sync_statements.sql                      # Source DB sync scripts
├── target_sync_statements.sql                      # Target DB sync scripts
└── detailed_comparison_report.html                 # Detailed HTML report

logs/
├── db_sentinel_20241209.log                        # Application logs
└── audit_trail.log                                 # Audit information
```

### **Sample Summary Report**
```
================================================================================
                            DB-SENTINEL COMPARISON REPORT
================================================================================
Job ID: dbsentinel_20241209_143022
Duration: 1,247.32 seconds

SUMMARY:
--------
Total Tables: 8
Successful: 8
Failed: 0
Success Rate: 100.0%

DATA STATISTICS:
---------------
Total Source Rows: 15,247,891
Total Target Rows: 15,249,123
Total Mismatches: 1,232
Data Accuracy: 99.99%

TABLE DETAILS:
--------------
✅ EMPLOYEES:
   Source Rows: 1,247
   Target Rows: 1,247
   Mismatches: 0
   Duration: 2.15s

✅ ORDER_ITEMS:
   Source Rows: 2,847,291
   Target Rows: 2,847,291
   Mismatches: 45
   Inserts Needed: 12
   Updates Needed: 33
   Duration: 187.42s
```

## 🔧 Advanced Features

### **Data Masking Integration**
```yaml
global_config:
  enable_data_masking: true

masking_config:
  rules:
    - table_pattern: ".*CUSTOMER.*"
      column_rules:
        - column_name: "EMAIL"
          masking_strategy: "email_domain_preserve"
        - column_name: "SSN"
          masking_strategy: "format_preserving_hash"
```

### **Performance Monitoring**
```yaml
monitoring:
  enabled: true
  metrics_port: 8080
  alert_on_failure: true
  mismatch_threshold_percent: 5.0
```

### **Integration Support**
```yaml
integrations:
  kafka:
    enabled: true
    bootstrap_servers: "localhost:9092"
    topic_prefix: "dbsentinel"
  
  prometheus:
    enabled: true
    push_gateway: "localhost:9091"
```

## 📈 Performance Guidelines

### **Chunk Size Recommendations**
| Table Size | Recommended Chunk Size | Reasoning |
|------------|----------------------|-----------|
| < 100K rows | 1,000 - 5,000 | Minimize overhead |
| 100K - 1M rows | 5,000 - 10,000 | Balance memory/performance |
| 1M - 10M rows | 10,000 - 20,000 | Optimize throughput |
| > 10M rows | 20,000 - 50,000 | Maximize efficiency |

### **Thread Count Guidelines**
| Scenario | Recommended Threads | Notes |
|----------|-------------------|-------|
| Small tables | 2-4 | Limited benefit from parallelism |
| Medium tables | 4-8 | Good balance |
| Large tables | 8-16 | Maximize CPU utilization |
| High latency DB | 2-6 | Reduce connection pressure |

### **Memory Optimization**
- **Estimate**: `chunk_size × max_threads × avg_row_size × 3`
- **Guideline**: Keep total memory under 8GB for stability
- **Large rows**: Reduce chunk_size for tables with wide rows

## 🚨 Best Practices

### **Configuration**
1. **Start with discovery**: Use auto-discovery to generate initial config
2. **Validate early**: Always validate configuration before large runs
3. **Test with samples**: Use WHERE clauses to test with subsets first
4. **Monitor memory**: Watch memory usage during initial runs

### **Performance**
1. **Database tuning**: Ensure proper indexes on primary key columns
2. **Network considerations**: Run close to databases for low latency
3. **Resource allocation**: Dedicate sufficient CPU and memory
4. **Parallel degree**: Consider Oracle parallel query settings

### **Operations**
1. **Checkpoint usage**: Enable restart for long-running comparisons
2. **Monitoring**: Set up alerts for failures and high mismatch rates
3. **Scheduling**: Use cron for regular automated comparisons
4. **Audit trails**: Maintain comprehensive logs for compliance

## 🔍 Troubleshooting

### **Common Issues**

#### Configuration Errors
```bash
# Error: Primary key column not found
python scripts/db_sentinel_utils.py validate config.yaml
# Fix: Check column names and case sensitivity
```

#### Performance Issues
```bash
# High memory usage
# Solution: Reduce chunk_size or max_threads
chunk_size: 5000  # Instead of 50000
max_threads: 4    # Instead of 16
```

#### Connection Problems
```bash
# Oracle connection timeout
# Solution: Increase connection_timeout
source_db:
  connection_timeout: 120  # Instead of 30
  query_timeout: 1800      # 30 minutes
```

### **Debugging Commands**
```bash
# Test database connectivity
python scripts/db_sentinel_utils.py analyze \
    --user test_user --password test_pass --dsn host:1521/db --schema TEST

# Validate specific table configuration
python scripts/db_sentinel_utils.py validate config.yaml

# Run with debug logging
export LOG_LEVEL=DEBUG
python db_sentinel.py config.yaml
```

## 🔗 Integration Examples

### **CI/CD Pipeline**
```yaml
# .github/workflows/data-validation.yml
- name: Run DB-Sentinel
  run: |
    python db_sentinel.py production_config.yaml
    if [ $? -ne 0 ]; then
      echo "Data comparison failed"
      exit 1
    fi
```

### **Cron Scheduling**
```bash
# Daily comparison at 2 AM
0 2 * * * cd /opt/db-sentinel && python db_sentinel.py daily_config.yaml
```

### **Monitoring Integration**
```bash
# Prometheus metrics endpoint
curl http://localhost:8080/metrics

# Grafana dashboard
# Import dashboard from monitoring/grafana/db-sentinel-dashboard.json
```

## 📚 Documentation

- [Configuration Reference](docs/CONFIGURATION.md) - Complete configuration guide
- [Performance Tuning](docs/PERFORMANCE.md) - Optimization strategies
- [API Documentation](docs/API.md) - Integration interfaces
- [Troubleshooting Guide](docs/TROUBLESHOOTING.md) - Common issues and solutions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Add tests for new functionality
4. Ensure all tests pass: `python -m pytest tests/`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- 📧 **Email**: support@yourcompany.com
- 📖 **Documentation**: [Wiki](https://github.com/yourorg/db-sentinel/wiki)
- 🐛 **Issues**: [GitHub Issues](https://github.com/yourorg/db-sentinel/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/yourorg/db-sentinel/discussions)

---

**⭐ If DB-Sentinel helps your organization, please give it a star!**

*Built with ❤️ for enterprise data teams*
