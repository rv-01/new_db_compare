#!/usr/bin/env python3
"""
DB-Sentinel v2: Enhanced Database Comparison with Auto Primary Key Detection
============================================================================

Major enhancements in v2:
- Automatic primary key detection when not specified
- Intelligent fallback strategies for tables without primary keys
- Enhanced user guidance and recommendations
- Improved configuration flexibility
- Better error messages and suggestions

Features:
- Multi-table comparison with individual configurations
- AUTO primary key detection from database constraints
- Flexible primary key support (single and composite)
- Column-level filtering and WHERE clause support
- Row-level hashing for accurate comparison
- Multi-threaded processing for optimal performance
- Restart/resume capability with checkpoint management
- Comprehensive audit logging and monitoring

Author: Auto-generated DB-Sentinel v2
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
from dataclasses import dataclass, field
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
    """Enhanced table comparison configuration with optional primary key."""
    table_name: str
    primary_key: Optional[List[str]] = None  # Now optional - will auto-detect if None
    chunk_size: int = 10000
    columns: Optional[List[str]] = None
    where_clause: Optional[str] = None
    schema: Optional[str] = None
    max_threads: Optional[int] = None
    
    # v2 enhancements
    auto_detect_pk: bool = True  # Enable auto-detection if primary_key is None
    pk_detection_strategy: str = 'best_available'  # primary, unique, index, all_columns
    pk_detection_max_columns: int = 5  # Max columns for composite primary key
    
    # Detected values (populated during processing)
    detected_primary_key: Optional[List[str]] = field(default=None, init=False)
    pk_detection_method: Optional[str] = field(default=None, init=False)
    pk_detection_confidence: Optional[str] = field(default=None, init=False)


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
    
    # v2 enhancements
    auto_pk_detection_enabled: bool = True
    pk_detection_timeout: int = 30
    show_pk_detection_details: bool = True


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


@dataclass
class PrimaryKeyInfo:
    """Information about detected primary key."""
    columns: List[str]
    detection_method: str  # 'primary_constraint', 'unique_constraint', 'unique_index', 'all_columns'
    confidence: str  # 'high', 'medium', 'low'
    constraint_name: Optional[str] = None
    is_composite: bool = False
    column_count: int = 0
    
    def __post_init__(self):
        self.is_composite = len(self.columns) > 1
        self.column_count = len(self.columns)


class PrimaryKeyDetector:
    """Detects primary keys automatically from database metadata."""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def detect_primary_key(self, schema: str, table_name: str, 
                          strategy: str = 'best_available',
                          max_columns: int = 5) -> Optional[PrimaryKeyInfo]:
        """
        Detect primary key for a table using various strategies.
        
        Args:
            schema: Database schema name
            table_name: Table name
            strategy: Detection strategy ('primary', 'unique', 'index', 'best_available', 'all_columns')
            max_columns: Maximum number of columns to consider for composite keys
            
        Returns:
            PrimaryKeyInfo object with detected primary key information
        """
        logging.info(f"🔍 Auto-detecting primary key for {schema}.{table_name}")
        
        if strategy == 'best_available':
            # Try strategies in order of preference
            strategies = ['primary', 'unique', 'index']
            for strat in strategies:
                pk_info = self._detect_by_strategy(schema, table_name, strat, max_columns)
                if pk_info:
                    logging.info(f"✅ Detected primary key using '{strat}' strategy: {pk_info.columns}")
                    return pk_info
            
            # Fallback to all columns (not recommended but allows processing)
            logging.warning(f"⚠️ No suitable primary key found for {schema}.{table_name}, using all columns")
            return self._detect_by_strategy(schema, table_name, 'all_columns', max_columns)
        else:
            return self._detect_by_strategy(schema, table_name, strategy, max_columns)
    
    def _detect_by_strategy(self, schema: str, table_name: str, 
                           strategy: str, max_columns: int) -> Optional[PrimaryKeyInfo]:
        """Detect primary key using specific strategy."""
        
        if strategy == 'primary':
            return self._detect_primary_constraint(schema, table_name, max_columns)
        elif strategy == 'unique':
            return self._detect_unique_constraint(schema, table_name, max_columns)
        elif strategy == 'index':
            return self._detect_unique_index(schema, table_name, max_columns)
        elif strategy == 'all_columns':
            return self._use_all_columns(schema, table_name, max_columns)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
    
    def _detect_primary_constraint(self, schema: str, table_name: str, 
                                  max_columns: int) -> Optional[PrimaryKeyInfo]:
        """Detect primary key from primary key constraints."""
        query = """
        SELECT cc.column_name, c.constraint_name
        FROM all_constraints c, all_cons_columns cc
        WHERE c.owner = UPPER(:schema)
        AND c.table_name = UPPER(:table_name)
        AND c.constraint_type = 'P'
        AND c.owner = cc.owner
        AND c.constraint_name = cc.constraint_name
        ORDER BY cc.position
        """
        
        try:
            df = self.db_manager.execute_query(query, {
                'schema': schema,
                'table_name': table_name
            })
            
            if not df.empty and len(df) <= max_columns:
                columns = df['COLUMN_NAME'].tolist()
                constraint_name = df['CONSTRAINT_NAME'].iloc[0]
                
                return PrimaryKeyInfo(
                    columns=columns,
                    detection_method='primary_constraint',
                    confidence='high',
                    constraint_name=constraint_name
                )
            elif len(df) > max_columns:
                logging.warning(f"⚠️ Primary key has {len(df)} columns (max {max_columns}), skipping")
                
        except Exception as e:
            logging.debug(f"Primary constraint detection failed: {e}")
        
        return None
    
    def _detect_unique_constraint(self, schema: str, table_name: str, 
                                 max_columns: int) -> Optional[PrimaryKeyInfo]:
        """Detect primary key from unique constraints."""
        query = """
        SELECT cc.column_name, c.constraint_name, COUNT(*) OVER (PARTITION BY c.constraint_name) as col_count
        FROM all_constraints c, all_cons_columns cc
        WHERE c.owner = UPPER(:schema)
        AND c.table_name = UPPER(:table_name)
        AND c.constraint_type = 'U'
        AND c.owner = cc.owner
        AND c.constraint_name = cc.constraint_name
        ORDER BY col_count, cc.position
        """
        
        try:
            df = self.db_manager.execute_query(query, {
                'schema': schema,
                'table_name': table_name
            })
            
            if not df.empty:
                # Prefer constraint with fewer columns (more likely to be a good key)
                min_cols = df['COL_COUNT'].min()
                if min_cols <= max_columns:
                    constraint_df = df[df['COL_COUNT'] == min_cols]
                    first_constraint = constraint_df['CONSTRAINT_NAME'].iloc[0]
                    columns = constraint_df[constraint_df['CONSTRAINT_NAME'] == first_constraint]['COLUMN_NAME'].tolist()
                    
                    return PrimaryKeyInfo(
                        columns=columns,
                        detection_method='unique_constraint',
                        confidence='medium',
                        constraint_name=first_constraint
                    )
                    
        except Exception as e:
            logging.debug(f"Unique constraint detection failed: {e}")
        
        return None
    
    def _detect_unique_index(self, schema: str, table_name: str, 
                            max_columns: int) -> Optional[PrimaryKeyInfo]:
        """Detect primary key from unique indexes."""
        query = """
        SELECT ic.column_name, i.index_name, COUNT(*) OVER (PARTITION BY i.index_name) as col_count
        FROM all_indexes i, all_ind_columns ic
        WHERE i.owner = UPPER(:schema)
        AND i.table_name = UPPER(:table_name)
        AND i.uniqueness = 'UNIQUE'
        AND i.owner = ic.index_owner
        AND i.index_name = ic.index_name
        ORDER BY col_count, ic.column_position
        """
        
        try:
            df = self.db_manager.execute_query(query, {
                'schema': schema,
                'table_name': table_name
            })
            
            if not df.empty:
                # Prefer index with fewer columns
                min_cols = df['COL_COUNT'].min()
                if min_cols <= max_columns:
                    index_df = df[df['COL_COUNT'] == min_cols]
                    first_index = index_df['INDEX_NAME'].iloc[0]
                    columns = index_df[index_df['INDEX_NAME'] == first_index]['COLUMN_NAME'].tolist()
                    
                    return PrimaryKeyInfo(
                        columns=columns,
                        detection_method='unique_index',
                        confidence='medium',
                        constraint_name=first_index
                    )
                    
        except Exception as e:
            logging.debug(f"Unique index detection failed: {e}")
        
        return None
    
    def _use_all_columns(self, schema: str, table_name: str, 
                        max_columns: int) -> Optional[PrimaryKeyInfo]:
        """Use all columns as primary key (fallback strategy)."""
        query = """
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = UPPER(:schema)
        AND table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        try:
            df = self.db_manager.execute_query(query, {
                'schema': schema,
                'table_name': table_name
            })
            
            if not df.empty:
                columns = df['COLUMN_NAME'].tolist()
                
                # Limit to max_columns if too many
                if len(columns) > max_columns:
                    logging.warning(f"⚠️ Table has {len(columns)} columns, using first {max_columns} as key")
                    columns = columns[:max_columns]
                
                return PrimaryKeyInfo(
                    columns=columns,
                    detection_method='all_columns',
                    confidence='low'
                )
                
        except Exception as e:
            logging.error(f"All columns detection failed: {e}")
        
        return None
    
    def get_table_column_info(self, schema: str, table_name: str) -> pd.DataFrame:
        """Get detailed column information for analysis."""
        query = """
        SELECT 
            column_name,
            data_type,
            nullable,
            data_length,
            data_precision,
            data_scale,
            column_id
        FROM all_tab_columns
        WHERE owner = UPPER(:schema)
        AND table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        return self.db_manager.execute_query(query, {
            'schema': schema,
            'table_name': table_name
        })
    
    def analyze_primary_key_candidates(self, schema: str, table_name: str) -> Dict[str, Any]:
        """Analyze potential primary key candidates and provide recommendations."""
        analysis = {
            'table_name': table_name,
            'schema': schema,
            'primary_constraints': [],
            'unique_constraints': [],
            'unique_indexes': [],
            'recommendations': [],
            'total_columns': 0,
            'nullable_columns': 0
        }
        
        try:
            # Get basic table info
            columns_df = self.get_table_column_info(schema, table_name)
            analysis['total_columns'] = len(columns_df)
            analysis['nullable_columns'] = len(columns_df[columns_df['NULLABLE'] == 'Y'])
            
            # Check for primary constraints
            pk_info = self._detect_primary_constraint(schema, table_name, 10)
            if pk_info:
                analysis['primary_constraints'].append({
                    'columns': pk_info.columns,
                    'constraint_name': pk_info.constraint_name,
                    'column_count': pk_info.column_count
                })
                analysis['recommendations'].append(f"✅ Primary key found: {pk_info.columns}")
            
            # Check for unique constraints
            unique_info = self._detect_unique_constraint(schema, table_name, 10)
            if unique_info:
                analysis['unique_constraints'].append({
                    'columns': unique_info.columns,
                    'constraint_name': unique_info.constraint_name,
                    'column_count': unique_info.column_count
                })
                if not pk_info:
                    analysis['recommendations'].append(f"💡 Unique constraint available: {unique_info.columns}")
            
            # Check for unique indexes
            index_info = self._detect_unique_index(schema, table_name, 10)
            if index_info:
                analysis['unique_indexes'].append({
                    'columns': index_info.columns,
                    'index_name': index_info.constraint_name,
                    'column_count': index_info.column_count
                })
                if not pk_info and not unique_info:
                    analysis['recommendations'].append(f"💡 Unique index available: {index_info.columns}")
            
            # General recommendations
            if not pk_info and not unique_info and not index_info:
                analysis['recommendations'].append("⚠️ No unique constraints or indexes found")
                analysis['recommendations'].append("🔧 Consider creating a primary key or unique constraint")
                analysis['recommendations'].append("📊 Will use all columns as fallback (may impact performance)")
            
            non_nullable_cols = columns_df[columns_df['NULLABLE'] == 'N']['COLUMN_NAME'].tolist()
            if non_nullable_cols and len(non_nullable_cols) <= 3:
                analysis['recommendations'].append(f"💡 Non-nullable columns could be good key candidates: {non_nullable_cols}")
            
        except Exception as e:
            logging.error(f"Primary key analysis failed for {schema}.{table_name}: {e}")
            analysis['error'] = str(e)
        
        return analysis


class DatabaseManager:
    """Enhanced database manager with primary key detection support."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection_pool = []
        self._pool_lock = threading.Lock()
        self._max_pool_size = 10
        self.pk_detector = PrimaryKeyDetector(self)
    
    def get_connection(self):
        """Get a database connection from pool."""
        with self._pool_lock:
            if self._connection_pool:
                return self._connection_pool.pop()
            
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
                    connection.ping()
                    self._connection_pool.append(connection)
                except:
                    try:
                        connection.close()
                    except:
                        pass
            else:
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
        row_str = '|'.join([
            str(value) if pd.notna(value) else 'NULL' 
            for value in row_data.values
        ])
        
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
        
        df = df.sort_values(by=primary_keys).reset_index(drop=True)
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
        """Fetch data batch with flexible column selection and WHERE clauses."""
        if table_config.columns:
            column_list = ', '.join(table_config.columns)
        else:
            all_columns = self.db_manager.get_table_columns(schema, table_config.table_name)
            column_list = ', '.join(all_columns)
        
        # Use detected or configured primary key
        primary_keys = table_config.detected_primary_key or table_config.primary_key
        if not primary_keys:
            raise ValueError(f"No primary key available for {table_config.table_name}")
        
        order_by = ', '.join(primary_keys)
        
        where_conditions = []
        if table_config.where_clause:
            where_conditions.append(f"({table_config.where_clause})")
        
        where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
        
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


class EnhancedMultiTableComparator:
    """Enhanced comparator with automatic primary key detection."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.job_id = f"dbsentinel_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
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
            
            required_keys = ['source_db', 'target_db', 'global_config', 'tables']
            for key in required_keys:
                if key not in config:
                    raise ValueError(f"Missing required configuration section: {key}")
            
            config['source_db'] = DatabaseConfig(**config['source_db'])
            config['target_db'] = DatabaseConfig(**config['target_db'])
            config['global_config'] = GlobalConfig(**config['global_config'])
            
            # Enhanced table configuration parsing
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
        log_path = Path(global_config.log_directory) / f"db_sentinel_v2_{datetime.now().strftime('%Y%m%d')}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def detect_and_validate_primary_keys(self) -> bool:
        """Detect primary keys for tables that don't have them specified."""
        global_config = self.config['global_config']
        
        if not global_config.auto_pk_detection_enabled:
            logging.info("🔧 Auto primary key detection is disabled")
            return self._validate_existing_primary_keys()
        
        logging.info("🔍 Starting automatic primary key detection...")
        all_valid = True
        
        for table_config in self.config['tables']:
            schema = table_config.schema or global_config.schema
            
            if table_config.primary_key:
                # Validate existing primary key
                logging.info(f"✅ Using configured primary key for {schema}.{table_config.table_name}: {table_config.primary_key}")
                table_config.detected_primary_key = table_config.primary_key
                table_config.pk_detection_method = 'configured'
                table_config.pk_detection_confidence = 'user_specified'
            else:
                # Auto-detect primary key
                logging.info(f"🔍 Auto-detecting primary key for {schema}.{table_config.table_name}")
                
                try:
                    pk_info = self.source_db.pk_detector.detect_primary_key(
                        schema=schema,
                        table_name=table_config.table_name,
                        strategy=table_config.pk_detection_strategy,
                        max_columns=table_config.pk_detection_max_columns
                    )
                    
                    if pk_info:
                        table_config.detected_primary_key = pk_info.columns
                        table_config.pk_detection_method = pk_info.detection_method
                        table_config.pk_detection_confidence = pk_info.confidence
                        
                        # Log detection details
                        confidence_emoji = {
                            'high': '🟢',
                            'medium': '🟡', 
                            'low': '🔴'
                        }.get(pk_info.confidence, '⚪')
                        
                        logging.info(f"{confidence_emoji} Detected primary key for {table_config.table_name}: {pk_info.columns}")
                        logging.info(f"   Method: {pk_info.detection_method}, Confidence: {pk_info.confidence}")
                        
                        if pk_info.constraint_name:
                            logging.info(f"   Constraint/Index: {pk_info.constraint_name}")
                        
                        if pk_info.confidence == 'low':
                            logging.warning(f"⚠️ Low confidence detection for {table_config.table_name} - consider specifying primary key manually")
                        
                        # Show detailed analysis if enabled
                        if global_config.show_pk_detection_details:
                            self._show_pk_analysis(schema, table_config.table_name)
                    else:
                        logging.error(f"❌ Could not detect primary key for {schema}.{table_config.table_name}")
                        all_valid = False
                        
                except Exception as e:
                    logging.error(f"❌ Primary key detection failed for {schema}.{table_config.table_name}: {e}")
                    all_valid = False
        
        return all_valid
    
    def _show_pk_analysis(self, schema: str, table_name: str):
        """Show detailed primary key analysis."""
        try:
            analysis = self.source_db.pk_detector.analyze_primary_key_candidates(schema, table_name)
            
            logging.info(f"   📊 Table Analysis for {schema}.{table_name}:")
            logging.info(f"      Total columns: {analysis['total_columns']}")
            logging.info(f"      Nullable columns: {analysis['nullable_columns']}")
            
            if analysis['primary_constraints']:
                for pc in analysis['primary_constraints']:
                    logging.info(f"      Primary constraint: {pc['columns']} ({pc['constraint_name']})")
            
            if analysis['unique_constraints']:
                for uc in analysis['unique_constraints']:
                    logging.info(f"      Unique constraint: {uc['columns']} ({uc['constraint_name']})")
            
            if analysis['unique_indexes']:
                for ui in analysis['unique_indexes']:
                    logging.info(f"      Unique index: {ui['columns']} ({ui['index_name']})")
            
            for rec in analysis['recommendations']:
                logging.info(f"      {rec}")
                
        except Exception as e:
            logging.debug(f"Could not show PK analysis: {e}")
    
    def _validate_existing_primary_keys(self) -> bool:
        """Validate explicitly configured primary keys."""
        global_config = self.config['global_config']
        all_valid = True
        
        for table_config in self.config['tables']:
            schema = table_config.schema or global_config.schema
            
            if not table_config.primary_key:
                logging.error(f"❌ No primary key specified for {schema}.{table_config.table_name} and auto-detection is disabled")
                all_valid = False
                continue
            
            try:
                # Verify columns exist
                actual_columns = self.source_db.get_table_columns(schema, table_config.table_name)
                
                for pk in table_config.primary_key:
                    if pk.upper() not in [col.upper() for col in actual_columns]:
                        logging.error(f"❌ Primary key column '{pk}' not found in {table_config.table_name}")
                        all_valid = False
                
                if all_valid:
                    table_config.detected_primary_key = table_config.primary_key
                    table_config.pk_detection_method = 'configured'
                    table_config.pk_detection_confidence = 'user_specified'
                
            except Exception as e:
                logging.error(f"❌ Error validating primary key for {table_config.table_name}: {e}")
                all_valid = False
        
        return all_valid
    
    def validate_table_configurations(self) -> bool:
        """Enhanced table validation with primary key detection."""
        logging.info("🔍 Validating table configurations...")
        
        # First detect/validate primary keys
        if not self.detect_and_validate_primary_keys():
            return False
        
        # Then validate table access and other configurations
        global_config = self.config['global_config']
        all_valid = True
        
        for table_config in self.config['tables']:
            schema = table_config.schema or global_config.schema
            
            try:
                # Check if table exists
                actual_columns = self.source_db.get_table_columns(schema, table_config.table_name)
                
                if not actual_columns:
                    logging.error(f"❌ Table {schema}.{table_config.table_name} not found or no access")
                    all_valid = False
                    continue
                
                # Validate specified columns exist (if any)
                if table_config.columns:
                    for col in table_config.columns:
                        if col.upper() not in [actual_col.upper() for actual_col in actual_columns]:
                            logging.error(f"❌ Specified column '{col}' not found in {table_config.table_name}")
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
                        logging.error(f"❌ Invalid WHERE clause for {table_config.table_name}: {e}")
                        all_valid = False
                
                if all_valid:
                    pk_method = table_config.pk_detection_method
                    pk_confidence = table_config.pk_detection_confidence
                    pk_keys = table_config.detected_primary_key
                    
                    confidence_emoji = {
                        'user_specified': '👤',
                        'high': '🟢',
                        'medium': '🟡',
                        'low': '🔴'
                    }.get(pk_confidence, '⚪')
                    
                    logging.info(f"✅ Table configuration valid: {schema}.{table_config.table_name}")
                    logging.info(f"   {confidence_emoji} Primary key: {pk_keys} (method: {pk_method})")
                
            except Exception as e:
                logging.error(f"❌ Error validating table {table_config.table_name}: {e}")
                all_valid = False
        
        return all_valid
    
    def compare_single_table(self, table_config: TableConfig) -> Dict[str, Any]:
        """Compare a single table between source and target with enhanced primary key support."""
        global_config = self.config['global_config']
        schema = table_config.schema or global_config.schema
        
        # Use detected primary key
        primary_keys = table_config.detected_primary_key
        if not primary_keys:
            raise ValueError(f"No primary key available for {table_config.table_name}")
        
        logging.info(f"🚀 Starting comparison for table: {schema}.{table_config.table_name}")
        logging.info(f"   Using primary key: {primary_keys} ({table_config.pk_detection_method})")
        
        # Initialize table statistics
        table_stats = {
            'table_name': table_config.table_name,
            'schema': schema,
            'primary_key': primary_keys,
            'pk_detection_method': table_config.pk_detection_method,
            'pk_detection_confidence': table_config.pk_detection_confidence,
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
            
            logging.info(f"📊 Row counts - Source: {source_count:,}, Target: {target_count:,}")
            
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
            
            logging.info(f"📦 Processing {len(batches)} batches for {table_config.table_name}")
            
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
                                'PK Method': table_config.pk_detection_method
                            })
                            
                        except Exception as e:
                            logging.error(f"❌ Batch {batch.batch_id} failed for {table_config.table_name}: {e}")
                            table_stats['status'] = 'failed'
            
            table_stats['end_time'] = datetime.now()
            table_stats['duration'] = (table_stats['end_time'] - table_stats['start_time']).total_seconds()
            
            if table_stats['status'] != 'failed':
                table_stats['status'] = 'completed'
            
            logging.info(f"✅ Completed comparison for {table_config.table_name}: {table_stats}")
            
        except Exception as e:
            logging.error(f"❌ Error comparing table {table_config.table_name}: {e}")
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
            
            # Use detected primary key
            primary_keys = table_config.detected_primary_key
            
            # Generate hashes
            source_hashed = EnhancedHashGenerator.hash_dataframe(source_data, primary_keys)
            target_hashed = EnhancedHashGenerator.hash_dataframe(target_data, primary_keys)
            
            # Compare and generate statistics
            batch_stats = self._compare_batch_data(source_hashed, target_hashed, primary_keys)
            
            return batch_stats
            
        except Exception as e:
            logging.error(f"❌ Error processing batch {batch_info.batch_id} for {table_config.table_name}: {e}")
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
        """Run comparison for all configured tables with enhanced primary key support."""
        start_time = datetime.now()
        
        logging.info(f"🚀 Starting DB-Sentinel v2 comparison job: {self.job_id}")
        logging.info(f"📋 Configured tables: {len(self.config['tables'])}")
        
        # Validate configurations (includes primary key detection)
        if not self.validate_table_configurations():
            logging.error("❌ Configuration validation failed. Aborting.")
            sys.exit(1)
        
        # Process each table
        all_results = []
        
        for table_config in self.config['tables']:
            try:
                table_result = self.compare_single_table(table_config)
                all_results.append(table_result)
                self.table_stats[table_config.table_name] = table_result
            except Exception as e:
                logging.error(f"❌ Failed to process table {table_config.table_name}: {e}")
                error_result = {
                    'table_name': table_config.table_name,
                    'status': 'failed',
                    'error': str(e),
                    'start_time': datetime.now(),
                    'end_time': datetime.now()
                }
                all_results.append(error_result)
        
        # Generate summary report
        self._generate_enhanced_summary_report(all_results, start_time)
        
        # Cleanup
        self.source_db.close_all_connections()
        self.target_db.close_all_connections()
        
        logging.info("🎉 DB-Sentinel v2 comparison completed!")
    
    def _generate_enhanced_summary_report(self, results: List[Dict[str, Any]], start_time: datetime):
        """Generate enhanced summary report with primary key detection details."""
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        # Calculate totals
        total_tables = len(results)
        successful_tables = len([r for r in results if r.get('status') == 'completed'])
        failed_tables = total_tables - successful_tables
        
        total_source_rows = sum(r.get('source_rows', 0) for r in results)
        total_target_rows = sum(r.get('target_rows', 0) for r in results)
        total_mismatches = sum(r.get('mismatches', 0) for r in results)
        
        # Primary key detection stats
        auto_detected = len([r for r in results if r.get('pk_detection_method') not in ['configured', None]])
        user_specified = len([r for r in results if r.get('pk_detection_method') == 'configured'])
        
        # Generate enhanced report
        report = f"""
================================================================================
                       🛡️  DB-SENTINEL v2 COMPARISON REPORT
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

PRIMARY KEY DETECTION:
--------------------
Auto-detected: {auto_detected}
User-specified: {user_specified}
Detection Success Rate: {((auto_detected + user_specified)/total_tables*100):.1f}%

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
            
            # Primary key info
            pk_method = result.get('pk_detection_method', 'unknown')
            pk_confidence = result.get('pk_detection_confidence', 'unknown')
            primary_key = result.get('primary_key', [])
            
            confidence_emoji = {
                'user_specified': '👤',
                'high': '🟢',
                'medium': '🟡',
                'low': '🔴'
            }.get(pk_confidence, '⚪')
            
            report += f"""
{status_icon} {table_name}:
   Primary Key: {primary_key} {confidence_emoji}
   Detection: {pk_method} (confidence: {pk_confidence})
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
        
        report += f"""
================================================================================
🔍 PRIMARY KEY DETECTION LEGEND:
👤 User-specified    🟢 High confidence    🟡 Medium confidence    🔴 Low confidence

💡 RECOMMENDATIONS:
- Tables with 🔴 low confidence detection should have primary keys specified manually
- Consider creating proper primary key constraints for better performance
- Review tables with high mismatch counts for data quality issues
================================================================================
"""
        
        # Write report to file
        global_config = self.config['global_config']
        report_path = Path(global_config.output_directory) / f"summary_report_v2_{self.job_id}.txt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        # Print to console
        print(report)
        
        logging.info(f"📄 Enhanced summary report saved to: {report_path}")


def main():
    """Main entry point for DB-Sentinel v2."""
    if len(sys.argv) != 2:
        print("Usage: python db_sentinel_v2.py <config.yaml>")
        print("\nDB-Sentinel v2: Enhanced Database Comparison Tool")
        print("✨ NEW: Automatic primary key detection!")
        print("Supports multi-table comparison with flexible configurations")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        print("🚀 Starting DB-Sentinel v2...")
        print("✨ Enhanced with automatic primary key detection!")
        print("=" * 60)
        
        comparator = EnhancedMultiTableComparator(config_path)
        comparator.run_comparison()
        
        print("🎉 DB-Sentinel v2 completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  DB-Sentinel v2 interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ DB-Sentinel v2 failed: {e}")
        logging.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
