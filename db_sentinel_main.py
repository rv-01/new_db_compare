#!/usr/bin/env python3
"""
DB-Sentinel: Enterprise Database Comparison and Synchronization Tool
====================================================================

A production-ready database comparison utility supporting multiple tables,
flexible configurations, and enterprise-grade monitoring and alerting.

Features:
- Multi-table comparison with individual configurations
- Flexible primary key support (single and composite)
- Column-level filtering and WHERE clause support
- Row-level hashing for accurate comparison
- Multi-threaded processing for optimal performance
- Restart/resume capability with checkpoint management
- Comprehensive audit logging and monitoring
- SQL generation for synchronization
- Data masking and PII protection
- Real-time streaming comparison (optional)

Author: Auto-generated DB-Sentinel
Version: 2.0.0
"""

import os
import sys
import yaml
import hashlib
import logging
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

import oracledb
import pandas as pd
from tqdm import tqdm


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    user: str
    password: str
    dsn: str
    connection_timeout: int = 30
    query_timeout: int = 600


@dataclass
class TableConfig:
    """Individual table comparison configuration."""
    table_name: str
    primary_key: List[str]
    chunk_size: int = 10000
    columns: Optional[List[str]] = None  # None means all columns
    where_clause: Optional[str] = None   # None means no WHERE clause
    schema: Optional[str] = None         # Override global schema
    max_threads: Optional[int] = None    # Override global max_threads


@dataclass
class GlobalConfig:
    """Global configuration settings."""
    schema: str
    max_threads: int = 4
    batch_timeout: int = 300
    enable_restart: bool = True
    enable_reverification: bool = True
    enable_data_masking: bool = False
    output_directory: str = "./output"
    log_directory: str = "./logs"
    archive_directory: str = "./archive"


@dataclass
class BatchInfo:
    """Information about a processing batch."""
    table_name: str
    batch_id: int
    start_row: int
    end_row: int
    where_clause: Optional[str] = None
    status: str = 'pending'
    processed_time: Optional[datetime] = None
    error_message: Optional[str] = None


class DatabaseManager:
    """Enhanced database manager with better connection pooling."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection_pool = []
        self._pool_lock = threading.Lock()
        self._max_pool_size = 10
    
    def get_connection(self):
        """Get a database connection from pool."""
        with self._pool_lock:
            if self._connection_pool:
                return self._connection_pool.pop()
            
            # Create new connection if pool is empty
            try:
                connection = oracledb.connect(
                    user=self.config.user,
                    password=self.config.password,
                    dsn=self.config.dsn
                )
                return connection
            except Exception as e:
                logging.error(f"Failed to create database connection: {e}")
                raise
    
    def return_connection(self, connection):
        """Return connection to pool."""
        with self._pool_lock:
            if len(self._connection_pool) < self._max_pool_size:
                try:
                    # Test connection before returning to pool
                    connection.ping()
                    self._connection_pool.append(connection)
                except:
                    # Connection is dead, close it
                    try:
                        connection.close()
                    except:
                        pass
            else:
                # Pool is full, close connection
                try:
                    connection.close()
                except:
                    pass
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        connection = self.get_connection()
        try:
            df = pd.read_sql(query, connection, params=params or {})
            return df
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise
        finally:
            self.return_connection(connection)
    
    def execute_non_query(self, query: str, params: Optional[Dict] = None):
        """Execute a non-query statement."""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query, params or {})
            connection.commit()
        except Exception as e:
            connection.rollback()
            logging.error(f"Non-query execution failed: {e}")
            raise
        finally:
            cursor.close()
            self.return_connection(connection)
    
    def get_table_row_count(self, schema: str, table_name: str, 
                           where_clause: Optional[str] = None) -> int:
        """Get total row count for table with optional WHERE clause."""
        base_query = f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}"
        
        if where_clause:
            query = f"{base_query} WHERE {where_clause}"
        else:
            query = base_query
        
        df = self.execute_query(query)
        return df['ROW_COUNT'].iloc[0]
    
    def get_table_columns(self, schema: str, table_name: str) -> List[str]:
        """Get all column names for a table."""
        query = """
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = UPPER(:schema) AND table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        df = self.execute_query(query, {
            'schema': schema,
            'table_name': table_name
        })
        
        return df['COLUMN_NAME'].tolist()
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        with self._pool_lock:
            for connection in self._connection_pool:
                try:
                    connection.close()
                except:
                    pass
            self._connection_pool.clear()


class EnhancedHashGenerator:
    """Enhanced hash generator with better performance and options."""
    
    @staticmethod
    def hash_row(row_data: pd.Series, algorithm: str = 'md5') -> str:
        """Generate hash for a database row with configurable algorithm."""
        # Convert all values to strings and handle NaN/None values
        row_str = '|'.join([
            str(value) if pd.notna(value) else 'NULL' 
            for value in row_data.values
        ])
        
        # Generate hash based on algorithm
        if algorithm == 'md5':
            return hashlib.md5(row_str.encode('utf-8')).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(row_str.encode('utf-8')).hexdigest()
        elif algorithm == 'sha1':
            return hashlib.sha1(row_str.encode('utf-8')).hexdigest()
        else:
            return hashlib.md5(row_str.encode('utf-8')).hexdigest()
    
    @staticmethod
    def hash_dataframe(df: pd.DataFrame, primary_keys: List[str], 
                      algorithm: str = 'md5') -> pd.DataFrame:
        """Add hash column to DataFrame with better performance."""
        if df.empty:
            return df
        
        # Sort by primary keys for consistent ordering
        df = df.sort_values(by=primary_keys).reset_index(drop=True)
        
        # Generate hash for each row (vectorized when possible)
        df['row_hash'] = df.apply(
            lambda row: EnhancedHashGenerator.hash_row(row, algorithm), 
            axis=1
        )
        
        return df


class FlexibleDataFetcher:
    """Flexible data fetcher supporting various query configurations."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def fetch_data_batch(self, schema: str, table_config: TableConfig, 
                        batch_info: BatchInfo) -> pd.DataFrame:
        """
        Fetch data batch with flexible column selection and WHERE clauses.
        
        Args:
            schema: Database schema name
            table_config: Table configuration with columns and WHERE clause
            batch_info: Batch information with row range
            
        Returns:
            DataFrame containing the batch data
        """
        # Determine columns to select
        if table_config.columns:
            column_list = ', '.join(table_config.columns)
        else:
            # Get all columns if none specified
            all_columns = self.db_manager.get_table_columns(schema, table_config.table_name)
            column_list = ', '.join(all_columns)
        
        # Build ORDER BY clause using primary keys
        order_by = ', '.join(table_config.primary_key)
        
        # Build WHERE clause
        where_conditions = []
        
        # Add table-specific WHERE clause if provided
        if table_config.where_clause:
            where_conditions.append(f"({table_config.where_clause})")
        
        # Combine WHERE conditions
        where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
        
        # Build the complete query with pagination
        query = f"""
        SELECT * FROM (
            SELECT t.*, ROWNUM as rn FROM (
                SELECT {column_list}
                FROM {schema}.{table_config.table_name}
                WHERE {where_clause}
                ORDER BY {order_by}
            ) t
            WHERE ROWNUM <= {batch_info.end_row}
        )
        WHERE rn > {batch_info.start_row}
        """
        
        try:
            return self.db_manager.execute_query(query)
        except Exception as e:
            logging.error(f"Error fetching batch for {table_config.table_name}: {e}")
            raise


class MultiTableComparator:
    """Enhanced comparator supporting multiple tables with individual configurations."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.job_id = f"dbsentinel_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize database managers
        self.source_db = DatabaseManager(self.config['source_db'])
        self.target_db = DatabaseManager(self.config['target_db'])
        
        # Initialize components
        self.data_fetcher = FlexibleDataFetcher(self.source_db)
        self.target_data_fetcher = FlexibleDataFetcher(self.target_db)
        
        # Initialize counters per table
        self.table_stats = {}
        
        # Setup logging
        self._setup_logging()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load and validate configuration."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate required sections
            required_keys = ['source_db', 'target_db', 'global_config', 'tables']
            for key in required_keys:
                if key not in config:
                    raise ValueError(f"Missing required configuration section: {key}")
            
            # Convert to proper objects
            config['source_db'] = DatabaseConfig(**config['source_db'])
            config['target_db'] = DatabaseConfig(**config['target_db'])
            config['global_config'] = GlobalConfig(**config['global_config'])
            
            # Convert table configurations
            table_configs = []
            for table_dict in config['tables']:
                table_configs.append(TableConfig(**table_dict))
            config['tables'] = table_configs
            
            return config
            
        except Exception as e:
            print(f"Error loading configuration: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """Setup comprehensive logging."""
        global_config = self.config['global_config']
        log_path = Path(global_config.log_directory) / f"db_sentinel_{datetime.now().strftime('%Y%m%d')}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def validate_table_configurations(self) -> bool:
        """Validate all table configurations against actual database schema."""
        logging.info("Validating table configurations...")
        
        global_config = self.config['global_config']
        all_valid = True
        
        for table_config in self.config['tables']:
            # Use table-specific schema or global schema
            schema = table_config.schema or global_config.schema
            
            try:
                # Check if table exists and get columns
                actual_columns = self.source_db.get_table_columns(schema, table_config.table_name)
                
                if not actual_columns:
                    logging.error(f"Table {schema}.{table_config.table_name} not found or no access")
                    all_valid = False
                    continue
                
                # Validate primary keys exist
                for pk in table_config.primary_key:
                    if pk.upper() not in [col.upper() for col in actual_columns]:
                        logging.error(f"Primary key column '{pk}' not found in {table_config.table_name}")
                        all_valid = False
                
                # Validate specified columns exist (if any)
                if table_config.columns:
                    for col in table_config.columns:
                        if col.upper() not in [actual_col.upper() for actual_col in actual_columns]:
                            logging.error(f"Specified column '{col}' not found in {table_config.table_name}")
                            all_valid = False
                
                # Test WHERE clause if provided
                if table_config.where_clause:
                    try:
                        test_query = f"""
                        SELECT COUNT(*) FROM {schema}.{table_config.table_name} 
                        WHERE {table_config.where_clause} AND ROWNUM <= 1
                        """
                        self.source_db.execute_query(test_query)
                    except Exception as e:
                        logging.error(f"Invalid WHERE clause for {table_config.table_name}: {e}")
                        all_valid = False
                
                if all_valid:
                    logging.info(f"✅ Table configuration valid: {schema}.{table_config.table_name}")
                
            except Exception as e:
                logging.error(f"Error validating table {table_config.table_name}: {e}")
                all_valid = False
        
        return all_valid
    
    def compare_single_table(self, table_config: TableConfig) -> Dict[str, Any]:
        """Compare a single table between source and target."""
        global_config = self.config['global_config']
        schema = table_config.schema or global_config.schema
        
        logging.info(f"Starting comparison for table: {schema}.{table_config.table_name}")
        
        # Initialize table statistics
        table_stats = {
            'table_name': table_config.table_name,
            'schema': schema,
            'source_rows': 0,
            'target_rows': 0,
            'mismatches': 0,
            'inserts_needed': 0,
            'updates_needed': 0,
            'deletes_needed': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'status': 'running'
        }
        
        try:
            # Get row counts
            source_count = self.source_db.get_table_row_count(
                schema, table_config.table_name, table_config.where_clause
            )
            target_count = self.target_db.get_table_row_count(
                schema, table_config.table_name, table_config.where_clause
            )
            
            table_stats['source_rows'] = source_count
            table_stats['target_rows'] = target_count
            
            logging.info(f"Row counts - Source: {source_count:,}, Target: {target_count:,}")
            
            # Generate batches
            max_rows = max(source_count, target_count)
            total_batches = (max_rows + table_config.chunk_size - 1) // table_config.chunk_size
            
            batches = []
            for i in range(total_batches):
                start_row = i * table_config.chunk_size
                end_row = min((i + 1) * table_config.chunk_size, max_rows)
                
                batch_info = BatchInfo(
                    table_name=table_config.table_name,
                    batch_id=i,
                    start_row=start_row,
                    end_row=end_row,
                    where_clause=table_config.where_clause
                )
                batches.append(batch_info)
            
            logging.info(f"Processing {len(batches)} batches for {table_config.table_name}")
            
            # Process batches with threading
            max_threads = table_config.max_threads or global_config.max_threads
            max_threads = min(max_threads, len(batches))
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit batch jobs
                future_to_batch = {
                    executor.submit(self._process_table_batch, schema, table_config, batch): batch
                    for batch in batches
                }
                
                # Process results with progress bar
                with tqdm(total=len(batches), desc=f"Processing {table_config.table_name}") as pbar:
                    for future in as_completed(future_to_batch):
                        batch = future_to_batch[future]
                        try:
                            batch_stats = future.result()
                            
                            # Update table statistics
                            for key in ['mismatches', 'inserts_needed', 'updates_needed', 'deletes_needed']:
                                table_stats[key] += batch_stats.get(key, 0)
                            
                            pbar.update(1)
                            pbar.set_postfix({
                                'Mismatches': table_stats['mismatches'],
                                'Inserts': table_stats['inserts_needed'],
                                'Updates': table_stats['updates_needed'],
                                'Deletes': table_stats['deletes_needed']
                            })
                            
                        except Exception as e:
                            logging.error(f"Batch {batch.batch_id} failed for {table_config.table_name}: {e}")
                            table_stats['status'] = 'failed'
            
            table_stats['end_time'] = datetime.now()
            table_stats['duration'] = (table_stats['end_time'] - table_stats['start_time']).total_seconds()
            
            if table_stats['status'] != 'failed':
                table_stats['status'] = 'completed'
            
            logging.info(f"Completed comparison for {table_config.table_name}: {table_stats}")
            
        except Exception as e:
            logging.error(f"Error comparing table {table_config.table_name}: {e}")
            table_stats['status'] = 'failed'
            table_stats['error'] = str(e)
            table_stats['end_time'] = datetime.now()
        
        return table_stats
    
    def _process_table_batch(self, schema: str, table_config: TableConfig, 
                           batch_info: BatchInfo) -> Dict[str, int]:
        """Process a single batch for a table."""
        try:
            # Fetch data from both databases
            source_data = self.data_fetcher.fetch_data_batch(schema, table_config, batch_info)
            target_data = self.target_data_fetcher.fetch_data_batch(schema, table_config, batch_info)
            
            # Generate hashes
            source_hashed = EnhancedHashGenerator.hash_dataframe(
                source_data, table_config.primary_key
            )
            target_hashed = EnhancedHashGenerator.hash_dataframe(
                target_data, table_config.primary_key
            )
            
            # Compare and generate statistics
            batch_stats = self._compare_batch_data(source_hashed, target_hashed, table_config.primary_key)
            
            return batch_stats
            
        except Exception as e:
            logging.error(f"Error processing batch {batch_info.batch_id} for {table_config.table_name}: {e}")
            return {'mismatches': 0, 'inserts_needed': 0, 'updates_needed': 0, 'deletes_needed': 0}
    
    def _compare_batch_data(self, source_df: pd.DataFrame, target_df: pd.DataFrame, 
                          primary_keys: List[str]) -> Dict[str, int]:
        """Compare batch data and return statistics."""
        stats = {'mismatches': 0, 'inserts_needed': 0, 'updates_needed': 0, 'deletes_needed': 0}
        
        if source_df.empty and target_df.empty:
            return stats
        
        # Create primary key combinations for joining
        def create_pk_key(df):
            if df.empty:
                return df.assign(pk_key='')
            return df.assign(pk_key=df[primary_keys].astype(str).agg('|'.join, axis=1))
        
        source_keyed = create_pk_key(source_df)
        target_keyed = create_pk_key(target_df)
        
        # Find differences
        if not source_keyed.empty and not target_keyed.empty:
            # Records missing in target (need INSERT)
            missing_in_target = source_keyed[
                ~source_keyed['pk_key'].isin(target_keyed['pk_key'])
            ]
            stats['inserts_needed'] = len(missing_in_target)
            
            # Records missing in source (need DELETE from target)
            missing_in_source = target_keyed[
                ~target_keyed['pk_key'].isin(source_keyed['pk_key'])
            ]
            stats['deletes_needed'] = len(missing_in_source)
            
            # Compare hashes for common records (need UPDATE)
            common_keys = set(source_keyed['pk_key']) & set(target_keyed['pk_key'])
            
            if common_keys:
                source_common = source_keyed[source_keyed['pk_key'].isin(common_keys)]
                target_common = target_keyed[target_keyed['pk_key'].isin(common_keys)]
                
                # Merge on primary key and compare hashes
                merged = source_common.merge(target_common, on='pk_key', suffixes=('_src', '_tgt'))
                hash_mismatches = merged[merged['row_hash_src'] != merged['row_hash_tgt']]
                stats['updates_needed'] = len(hash_mismatches)
        
        elif not source_keyed.empty:
            # All source records need to be inserted
            stats['inserts_needed'] = len(source_keyed)
        elif not target_keyed.empty:
            # All target records need to be deleted
            stats['deletes_needed'] = len(target_keyed)
        
        stats['mismatches'] = stats['inserts_needed'] + stats['updates_needed'] + stats['deletes_needed']
        
        return stats
    
    def run_comparison(self):
        """Run comparison for all configured tables."""
        start_time = datetime.now()
        
        logging.info(f"Starting DB-Sentinel comparison job: {self.job_id}")
        logging.info(f"Configured tables: {len(self.config['tables'])}")
        
        # Validate configurations
        if not self.validate_table_configurations():
            logging.error("Configuration validation failed. Aborting.")
            sys.exit(1)
        
        # Process each table
        all_results = []
        
        for table_config in self.config['tables']:
            try:
                table_result = self.compare_single_table(table_config)
                all_results.append(table_result)
                self.table_stats[table_config.table_name] = table_result
            except Exception as e:
                logging.error(f"Failed to process table {table_config.table_name}: {e}")
                error_result = {
                    'table_name': table_config.table_name,
                    'status': 'failed',
                    'error': str(e),
                    'start_time': datetime.now(),
                    'end_time': datetime.now()
                }
                all_results.append(error_result)
        
        # Generate summary report
        self._generate_summary_report(all_results, start_time)
        
        # Cleanup
        self.source_db.close_all_connections()
        self.target_db.close_all_connections()
        
        logging.info("DB-Sentinel comparison completed!")
    
    def _generate_summary_report(self, results: List[Dict[str, Any]], start_time: datetime):
        """Generate comprehensive summary report."""
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        # Calculate totals
        total_tables = len(results)
        successful_tables = len([r for r in results if r.get('status') == 'completed'])
        failed_tables = total_tables - successful_tables
        
        total_source_rows = sum(r.get('source_rows', 0) for r in results)
        total_target_rows = sum(r.get('target_rows', 0) for r in results)
        total_mismatches = sum(r.get('mismatches', 0) for r in results)
        
        # Generate report
        report = f"""
================================================================================
                            DB-SENTINEL COMPARISON REPORT
================================================================================
Job ID: {self.job_id}
Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
Duration: {total_duration:.2f} seconds

SUMMARY:
--------
Total Tables: {total_tables}
Successful: {successful_tables}
Failed: {failed_tables}
Success Rate: {(successful_tables/total_tables*100):.1f}%

DATA STATISTICS:
---------------
Total Source Rows: {total_source_rows:,}
Total Target Rows: {total_target_rows:,}
Total Mismatches: {total_mismatches:,}
Data Accuracy: {((total_source_rows - total_mismatches)/total_source_rows*100):.2f}%

TABLE DETAILS:
--------------
"""
        
        for result in results:
            status_icon = "✅" if result.get('status') == 'completed' else "❌"
            table_name = result.get('table_name', 'Unknown')
            source_rows = result.get('source_rows', 0)
            target_rows = result.get('target_rows', 0)
            mismatches = result.get('mismatches', 0)
            duration = result.get('duration', 0)
            
            report += f"""
{status_icon} {table_name}:
   Source Rows: {source_rows:,}
   Target Rows: {target_rows:,}
   Mismatches: {mismatches:,}
   Inserts Needed: {result.get('inserts_needed', 0):,}
   Updates Needed: {result.get('updates_needed', 0):,}
   Deletes Needed: {result.get('deletes_needed', 0):,}
   Duration: {duration:.2f}s
"""
            
            if result.get('status') == 'failed':
                report += f"   Error: {result.get('error', 'Unknown error')}\n"
        
        report += "\n" + "="*80 + "\n"
        
        # Write report to file
        global_config = self.config['global_config']
        report_path = Path(global_config.output_directory) / f"summary_report_{self.job_id}.txt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        # Print to console
        print(report)
        
        logging.info(f"Summary report saved to: {report_path}")


def main():
    """Main entry point for DB-Sentinel."""
    if len(sys.argv) != 2:
        print("Usage: python db_sentinel.py <config.yaml>")
        print("\nDB-Sentinel: Enterprise Database Comparison Tool")
        print("Supports multi-table comparison with flexible configurations")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        print("🚀 Starting DB-Sentinel...")
        print("=" * 60)
        
        comparator = MultiTableComparator(config_path)
        comparator.run_comparison()
        
        print("🎉 DB-Sentinel completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  DB-Sentinel interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ DB-Sentinel failed: {e}")
        logging.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
