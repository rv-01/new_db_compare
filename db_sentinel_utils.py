#!/usr/bin/env python3
"""
DB-Sentinel Utility Scripts
===========================

Comprehensive utility functions for DB-Sentinel including:
- Table discovery and configuration generation
- Configuration validation and optimization
- Database schema analysis
- Performance tuning recommendations
- Quick comparison preview

Usage:
    python scripts/db_sentinel_utils.py [command] [options]

Commands:
    discover     - Discover tables and generate configuration
    validate     - Validate existing configuration
    analyze      - Analyze database schema and performance
    preview      - Quick preview comparison (sample data)
    optimize     - Optimize configuration for performance
"""

import sys
import os
import argparse
import yaml
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

import oracledb
from db_sentinel import DatabaseManager, DatabaseConfig


class TableDiscovery:
    """Discovers tables and generates DB-Sentinel configurations."""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_manager = DatabaseManager(db_config)
    
    def discover_schema_tables(self, schema: str, table_patterns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Discover all tables in a schema with their metadata."""
        query = """
        SELECT 
            t.table_name,
            t.num_rows,
            t.blocks,
            t.avg_row_len,
            t.last_analyzed,
            COUNT(c.column_name) as column_count,
            CASE 
                WHEN t.num_rows < 10000 THEN 1000
                WHEN t.num_rows < 100000 THEN 5000
                WHEN t.num_rows < 1000000 THEN 10000
                WHEN t.num_rows < 10000000 THEN 20000
                ELSE 25000
            END as suggested_chunk_size
        FROM all_tables t
        LEFT JOIN all_tab_columns c ON t.owner = c.owner AND t.table_name = c.table_name
        WHERE t.owner = UPPER(:schema)
        """
        
        # Add table pattern filtering if specified
        if table_patterns:
            pattern_conditions = []
            for i, pattern in enumerate(table_patterns):
                pattern_conditions.append(f"t.table_name LIKE :pattern_{i}")
            
            query += " AND (" + " OR ".join(pattern_conditions) + ")"
            
            params = {'schema': schema}
            for i, pattern in enumerate(table_patterns):
                params[f'pattern_{i}'] = pattern.upper()
        else:
            params = {'schema': schema}
        
        query += " GROUP BY t.table_name, t.num_rows, t.blocks, t.avg_row_len, t.last_analyzed"
        query += " ORDER BY t.num_rows DESC NULLS LAST"
        
        df = self.db_manager.execute_query(query, params)
        return df.to_dict('records')
    
    def get_table_primary_keys(self, schema: str, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        query = """
        SELECT cc.column_name
        FROM all_constraints c, all_cons_columns cc
        WHERE c.owner = UPPER(:schema)
        AND c.table_name = UPPER(:table_name)
        AND c.constraint_type = 'P'
        AND c.owner = cc.owner
        AND c.constraint_name = cc.constraint_name
        ORDER BY cc.position
        """
        
        df = self.db_manager.execute_query(query, {
            'schema': schema,
            'table_name': table_name
        })
        
        return df['COLUMN_NAME'].tolist()
    
    def get_table_columns(self, schema: str, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a table."""
        query = """
        SELECT 
            column_name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            column_id
        FROM all_tab_columns
        WHERE owner = UPPER(:schema)
        AND table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        df = self.db_manager.execute_query(query, {
            'schema': schema,
            'table_name': table_name
        })
        
        return df.to_dict('records')
    
    def analyze_table_characteristics(self, schema: str, table_name: str) -> Dict[str, Any]:
        """Analyze table characteristics for optimization recommendations."""
        # Get basic table stats
        table_info = self.discover_schema_tables(schema, [table_name])
        if not table_info:
            return {}
        
        table_info = table_info[0]
        
        # Get primary keys
        primary_keys = self.get_table_primary_keys(schema, table_name)
        
        # Get column details
        columns = self.get_table_columns(schema, table_name)
        
        # Analyze data distribution (sample)
        try:
            sample_query = f"""
            SELECT COUNT(*) as total_rows,
                   COUNT(DISTINCT {', '.join(primary_keys) if primary_keys else 'ROWNUM'}) as unique_keys
            FROM {schema}.{table_name}
            WHERE ROWNUM <= 10000
            """
            
            sample_df = self.db_manager.execute_query(sample_query)
            sample_stats = sample_df.iloc[0].to_dict()
        except:
            sample_stats = {}
        
        return {
            'table_info': table_info,
            'primary_keys': primary_keys,
            'columns': columns,
            'sample_stats': sample_stats,
            'recommendations': self._generate_table_recommendations(table_info, primary_keys, columns)
        }
    
    def _generate_table_recommendations(self, table_info: Dict, primary_keys: List[str], 
                                      columns: List[Dict]) -> Dict[str, Any]:
        """Generate optimization recommendations for a table."""
        recommendations = {
            'chunk_size': table_info.get('SUGGESTED_CHUNK_SIZE', 10000),
            'max_threads': 4,
            'include_all_columns': True,
            'suggested_where_clause': None,
            'performance_notes': []
        }
        
        num_rows = table_info.get('NUM_ROWS', 0) or 0
        column_count = table_info.get('COLUMN_COUNT', 0) or 0
        avg_row_len = table_info.get('AVG_ROW_LEN', 0) or 0
        
        # Adjust thread count based on table size
        if num_rows > 10000000:  # > 10M rows
            recommendations['max_threads'] = 8
            recommendations['performance_notes'].append("Large table: Consider higher thread count")
        elif num_rows > 1000000:  # > 1M rows
            recommendations['max_threads'] = 6
        elif num_rows < 100000:  # < 100K rows
            recommendations['max_threads'] = 2
            recommendations['performance_notes'].append("Small table: Limited parallelism beneficial")
        
        # Adjust chunk size based on row length
        if avg_row_len > 2000:  # Large rows
            recommendations['chunk_size'] = max(1000, recommendations['chunk_size'] // 2)
            recommendations['performance_notes'].append("Large rows: Reduced chunk size recommended")
        
        # Column recommendations
        if column_count > 50:
            recommendations['include_all_columns'] = False
            recommendations['performance_notes'].append("Many columns: Consider selecting only necessary columns")
        
        # Primary key analysis
        if not primary_keys:
            recommendations['performance_notes'].append("WARNING: No primary key found - comparison may be unreliable")
        elif len(primary_keys) > 3:
            recommendations['performance_notes'].append("Complex primary key: May impact performance")
        
        # Date-based filtering suggestions
        date_columns = [col['COLUMN_NAME'] for col in columns 
                       if 'DATE' in col['DATA_TYPE'] or 
                          any(keyword in col['COLUMN_NAME'].upper() 
                              for keyword in ['DATE', 'TIME', 'CREATED', 'UPDATED', 'MODIFIED'])]
        
        if date_columns:
            recommendations['suggested_where_clause'] = f"{date_columns[0]} >= SYSDATE - 30"
            recommendations['performance_notes'].append(f"Consider date filtering on {date_columns[0]}")
        
        return recommendations
    
    def generate_configuration(self, schema: str, tables: Optional[List[str]] = None,
                             table_patterns: Optional[List[str]] = None,
                             include_recommendations: bool = True) -> Dict[str, Any]:
        """Generate a complete DB-Sentinel configuration."""
        
        # Discover tables
        if tables:
            # Specific tables requested
            discovered_tables = []
            for table in tables:
                table_info = self.discover_schema_tables(schema, [table])
                if table_info:
                    discovered_tables.extend(table_info)
        else:
            # Discover all tables or by pattern
            discovered_tables = self.discover_schema_tables(schema, table_patterns)
        
        # Generate table configurations
        table_configs = []
        
        for table_info in discovered_tables:
            table_name = table_info['TABLE_NAME']
            
            if include_recommendations:
                analysis = self.analyze_table_characteristics(schema, table_name)
                recommendations = analysis.get('recommendations', {})
                primary_keys = analysis.get('primary_keys', [])
            else:
                primary_keys = self.get_table_primary_keys(schema, table_name)
                recommendations = {'chunk_size': table_info.get('SUGGESTED_CHUNK_SIZE', 10000)}
            
            if not primary_keys:
                logging.warning(f"Table {table_name} has no primary key - skipping")
                continue
            
            table_config = {
                'table_name': table_name,
                'primary_key': primary_keys,
                'chunk_size': recommendations.get('chunk_size', 10000)
            }
            
            # Add optional configurations based on recommendations
            if not recommendations.get('include_all_columns', True):
                # Get first 20 columns as example
                columns = self.get_table_columns(schema, table_name)
                table_config['columns'] = [col['COLUMN_NAME'] for col in columns[:20]]
            
            if recommendations.get('suggested_where_clause'):
                table_config['where_clause'] = recommendations['suggested_where_clause']
            
            if recommendations.get('max_threads', 4) != 4:
                table_config['max_threads'] = recommendations['max_threads']
            
            # Add comments with performance notes
            if recommendations.get('performance_notes'):
                table_config['_performance_notes'] = recommendations['performance_notes']
            
            table_configs.append(table_config)
        
        # Generate complete configuration
        config = {
            'source_db': {
                'user': 'source_user',
                'password': 'source_password', 
                'dsn': 'source_host:1521/SOURCEDB',
                'connection_timeout': 60,
                'query_timeout': 900
            },
            'target_db': {
                'user': 'target_user',
                'password': 'target_password',
                'dsn': 'target_host:1521/TARGETDB', 
                'connection_timeout': 60,
                'query_timeout': 900
            },
            'global_config': {
                'schema': schema,
                'max_threads': 6,
                'batch_timeout': 600,
                'enable_restart': True,
                'enable_reverification': True,
                'enable_data_masking': False,
                'output_directory': './output',
                'log_directory': './logs',
                'archive_directory': './archive'
            },
            'tables': table_configs
        }
        
        return config


class ConfigValidator:
    """Validates DB-Sentinel configurations."""
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
    
    def validate_configuration(self, config_path: str) -> Tuple[bool, List[str], List[str]]:
        """Validate a DB-Sentinel configuration file."""
        self.validation_errors = []
        self.validation_warnings = []
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            self.validation_errors.append(f"Failed to load configuration file: {e}")
            return False, self.validation_errors, self.validation_warnings
        
        # Validate structure
        self._validate_structure(config)
        
        # Validate database configurations
        self._validate_database_config(config.get('source_db', {}), 'source_db')
        self._validate_database_config(config.get('target_db', {}), 'target_db')
        
        # Validate global configuration
        self._validate_global_config(config.get('global_config', {}))
        
        # Validate table configurations
        self._validate_table_configs(config.get('tables', []))
        
        # Performance validation
        self._validate_performance_settings(config)
        
        is_valid = len(self.validation_errors) == 0
        return is_valid, self.validation_errors, self.validation_warnings
    
    def _validate_structure(self, config: Dict[str, Any]):
        """Validate basic configuration structure."""
        required_sections = ['source_db', 'target_db', 'global_config', 'tables']
        
        for section in required_sections:
            if section not in config:
                self.validation_errors.append(f"Missing required section: {section}")
        
        if not isinstance(config.get('tables'), list):
            self.validation_errors.append("'tables' must be a list")
        elif len(config.get('tables', [])) == 0:
            self.validation_errors.append("At least one table must be configured")
    
    def _validate_database_config(self, db_config: Dict[str, Any], config_name: str):
        """Validate database connection configuration."""
        required_fields = ['user', 'password', 'dsn']
        
        for field in required_fields:
            if field not in db_config:
                self.validation_errors.append(f"{config_name}.{field} is required")
        
        # Validate DSN format
        dsn = db_config.get('dsn', '')
        if dsn and ':' not in dsn:
            self.validation_warnings.append(f"{config_name}.dsn should include port (host:port/service)")
    
    def _validate_global_config(self, global_config: Dict[str, Any]):
        """Validate global configuration settings."""
        required_fields = ['schema']
        
        for field in required_fields:
            if field not in global_config:
                self.validation_errors.append(f"global_config.{field} is required")
        
        # Validate numeric settings
        max_threads = global_config.get('max_threads', 4)
        if not isinstance(max_threads, int) or max_threads < 1 or max_threads > 32:
            self.validation_errors.append("global_config.max_threads must be between 1 and 32")
    
    def _validate_table_configs(self, table_configs: List[Dict[str, Any]]):
        """Validate individual table configurations."""
        table_names = set()
        
        for i, table_config in enumerate(table_configs):
            # Check required fields
            if 'table_name' not in table_config:
                self.validation_errors.append(f"tables[{i}].table_name is required")
                continue
            
            table_name = table_config['table_name']
            
            # Check for duplicate table names
            if table_name in table_names:
                self.validation_errors.append(f"Duplicate table name: {table_name}")
            table_names.add(table_name)
            
            # Validate primary key
            if 'primary_key' not in table_config:
                self.validation_errors.append(f"tables[{i}] ({table_name}).primary_key is required")
            elif not isinstance(table_config['primary_key'], list) or len(table_config['primary_key']) == 0:
                self.validation_errors.append(f"tables[{i}] ({table_name}).primary_key must be a non-empty list")
            
            # Validate chunk size
            chunk_size = table_config.get('chunk_size', 10000)
            if not isinstance(chunk_size, int) or chunk_size < 100 or chunk_size > 100000:
                self.validation_warnings.append(f"tables[{i}] ({table_name}).chunk_size should be between 100 and 100,000")
            
            # Validate columns if specified
            columns = table_config.get('columns')
            if columns is not None:
                if not isinstance(columns, list) or len(columns) == 0:
                    self.validation_errors.append(f"tables[{i}] ({table_name}).columns must be a non-empty list if specified")
                else:
                    # Check that primary key columns are included
                    primary_keys = table_config.get('primary_key', [])
                    missing_pk_columns = [pk for pk in primary_keys if pk not in columns]
                    if missing_pk_columns:
                        self.validation_errors.append(
                            f"tables[{i}] ({table_name}).columns must include primary key columns: {missing_pk_columns}"
                        )
            
            # Validate max_threads if specified
            max_threads = table_config.get('max_threads')
            if max_threads is not None:
                if not isinstance(max_threads, int) or max_threads < 1 or max_threads > 32:
                    self.validation_warnings.append(f"tables[{i}] ({table_name}).max_threads should be between 1 and 32")
    
    def _validate_performance_settings(self, config: Dict[str, Any]):
        """Validate performance-related settings."""
        tables = config.get('tables', [])
        total_estimated_memory = 0
        
        for table_config in tables:
            chunk_size = table_config.get('chunk_size', 10000)
            max_threads = table_config.get('max_threads', config.get('global_config', {}).get('max_threads', 4))
            
            # Estimate memory usage (rough calculation)
            estimated_memory_mb = (chunk_size * max_threads * 500) / (1024 * 1024)  # 500 bytes per row estimate
            total_estimated_memory += estimated_memory_mb
        
        if total_estimated_memory > 8192:  # 8GB
            self.validation_warnings.append(
                f"High memory usage estimated ({total_estimated_memory:.1f} MB). "
                "Consider reducing chunk_size or max_threads."
            )


def cmd_discover(args):
    """Discover tables and generate configuration."""
    print("🔍 Discovering database tables...")
    
    # Create database config from args
    db_config = DatabaseConfig(
        user=args.user,
        password=args.password,
        dsn=args.dsn
    )
    
    discovery = TableDiscovery(db_config)
    
    try:
        # Generate configuration
        config = discovery.generate_configuration(
            schema=args.schema,
            tables=args.tables,
            table_patterns=args.patterns,
            include_recommendations=not args.no_recommendations
        )
        
        # Save configuration
        output_file = args.output or f"dbsentinel_config_{args.schema.lower()}.yaml"
        
        with open(output_file, 'w') as f:
            # Add header comment
            f.write(f"# DB-Sentinel Configuration\n")
            f.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Schema: {args.schema}\n")
            f.write(f"# Tables discovered: {len(config['tables'])}\n\n")
            
            yaml.dump(config, f, default_flow_style=False, indent=2, sort_keys=False)
        
        print(f"✅ Configuration generated: {output_file}")
        print(f"📊 Tables discovered: {len(config['tables'])}")
        
        # Show summary
        for table_config in config['tables']:
            table_name = table_config['table_name']
            chunk_size = table_config['chunk_size']
            pk_count = len(table_config['primary_key'])
            
            notes = table_config.get('_performance_notes', [])
            notes_str = f" ({', '.join(notes)})" if notes else ""
            
            print(f"  • {table_name}: chunk_size={chunk_size}, pk_columns={pk_count}{notes_str}")
    
    except Exception as e:
        print(f"❌ Discovery failed: {e}")
        sys.exit(1)


def cmd_validate(args):
    """Validate configuration file."""
    print(f"🔍 Validating configuration: {args.config}")
    
    validator = ConfigValidator()
    is_valid, errors, warnings = validator.validate_configuration(args.config)
    
    if errors:
        print("\n❌ Validation Errors:")
        for error in errors:
            print(f"  • {error}")
    
    if warnings:
        print("\n⚠️  Validation Warnings:")
        for warning in warnings:
            print(f"  • {warning}")
    
    if is_valid:
        print("\n✅ Configuration is valid!")
    else:
        print(f"\n❌ Configuration has {len(errors)} errors")
        sys.exit(1)


def cmd_analyze(args):
    """Analyze database schema."""
    print(f"📊 Analyzing schema: {args.schema}")
    
    db_config = DatabaseConfig(
        user=args.user,
        password=args.password,
        dsn=args.dsn
    )
    
    discovery = TableDiscovery(db_config)
    
    try:
        # Discover all tables
        tables = discovery.discover_schema_tables(args.schema)
        
        print(f"\n📋 Schema Analysis Results:")
        print(f"Schema: {args.schema}")
        print(f"Total tables: {len(tables)}")
        
        # Analyze by size categories
        small_tables = [t for t in tables if (t.get('NUM_ROWS') or 0) < 100000]
        medium_tables = [t for t in tables if 100000 <= (t.get('NUM_ROWS') or 0) < 1000000]
        large_tables = [t for t in tables if (t.get('NUM_ROWS') or 0) >= 1000000]
        
        print(f"Small tables (< 100K rows): {len(small_tables)}")
        print(f"Medium tables (100K - 1M rows): {len(medium_tables)}")
        print(f"Large tables (> 1M rows): {len(large_tables)}")
        
        # Show top 10 largest tables
        print(f"\n🔝 Top 10 Largest Tables:")
        sorted_tables = sorted(tables, key=lambda x: x.get('NUM_ROWS') or 0, reverse=True)[:10]
        
        for table in sorted_tables:
            name = table['TABLE_NAME']
            rows = table.get('NUM_ROWS') or 0
            analyzed = table.get('LAST_ANALYZED')
            analyzed_str = analyzed.strftime('%Y-%m-%d') if analyzed else 'Never'
            
            print(f"  {name:30} {rows:>10,} rows (analyzed: {analyzed_str})")
    
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        sys.exit(1)


def cmd_preview(args):
    """Quick preview comparison."""
    print(f"👀 Preview comparison: {args.config}")
    
    # This would implement a quick sample-based comparison
    print("Preview functionality coming soon...")


def cmd_optimize(args):
    """Optimize configuration for performance."""
    print(f"⚡ Optimizing configuration: {args.config}")
    
    # This would implement configuration optimization
    print("Optimization functionality coming soon...")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="DB-Sentinel Utility Scripts",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover tables and generate configuration')
    discover_parser.add_argument('--user', required=True, help='Database username')
    discover_parser.add_argument('--password', required=True, help='Database password')
    discover_parser.add_argument('--dsn', required=True, help='Database DSN')
    discover_parser.add_argument('--schema', required=True, help='Schema to analyze')
    discover_parser.add_argument('--tables', nargs='+', help='Specific tables to include')
    discover_parser.add_argument('--patterns', nargs='+', help='Table name patterns (e.g., "USER_%")')
    discover_parser.add_argument('--output', help='Output configuration file')
    discover_parser.add_argument('--no-recommendations', action='store_true', help='Skip performance recommendations')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configuration file')
    validate_parser.add_argument('config', help='Configuration file to validate')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze database schema')
    analyze_parser.add_argument('--user', required=True, help='Database username')
    analyze_parser.add_argument('--password', required=True, help='Database password')
    analyze_parser.add_argument('--dsn', required=True, help='Database DSN')
    analyze_parser.add_argument('--schema', required=True, help='Schema to analyze')
    
    # Preview command
    preview_parser = subparsers.add_parser('preview', help='Quick preview comparison')
    preview_parser.add_argument('config', help='Configuration file')
    
    # Optimize command
    optimize_parser = subparsers.add_parser('optimize', help='Optimize configuration')
    optimize_parser.add_argument('config', help='Configuration file to optimize')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # Execute command
    if args.command == 'discover':
        cmd_discover(args)
    elif args.command == 'validate':
        cmd_validate(args)
    elif args.command == 'analyze':
        cmd_analyze(args)
    elif args.command == 'preview':
        cmd_preview(args)
    elif args.command == 'optimize':
        cmd_optimize(args)


if __name__ == "__main__":
    main()
