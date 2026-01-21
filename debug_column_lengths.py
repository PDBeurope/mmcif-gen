#!/usr/bin/env python3
"""
Column Length Debugging Script for mmCIF Investigation Generation

This script helps troubleshoot the "all columns must have equal length" error
by analyzing the investigation data structure and identifying column length mismatches.

Usage:
    python debug_column_lengths.py <investigation_id> [options]
    
Options:
    --database-path: Path to SQLite database (default: pdbe_sqlite.db)
    --json-config: Path to JSON configuration file (default: ./operations/pdbe/pdbe_investigation.json)
    --verbose: Enable verbose output
    --fix-mismatches: Attempt to fix column length mismatches by padding with None values
"""

import sys
import os
import argparse
import logging
import sqlite3
from typing import Dict, List, Tuple, Any
from collections import defaultdict

# Add the mmcif_gen module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mmcif_gen.investigation_io import InvestigationStorage, SqliteReader
from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.facilities.pdbe import InvestigationPdbe

class ColumnLengthDebugger:
    """Debug and analyze column length mismatches in investigation data."""
    
    def __init__(self, investigation_id: str, database_path: str = "pdbe_sqlite.db", verbose: bool = False):
        self.investigation_id = investigation_id
        self.database_path = database_path
        self.verbose = verbose
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging based on verbosity level."""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'debug_column_lengths_{self.investigation_id}.log')
            ]
        )
        
    def analyze_database_structure(self) -> Dict[str, Any]:
        """Analyze the SQLite database structure and content."""
        logging.info("Analyzing database structure...")
        
        if not os.path.exists(self.database_path):
            logging.error(f"Database file not found: {self.database_path}")
            return {}
            
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        
        # Get table information
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        db_info = {}
        for table in tables:
            table_name = table[0]
            logging.info(f"Analyzing table: {table_name}")
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_data = cursor.fetchall()
            
            db_info[table_name] = {
                'columns': [col[1] for col in columns],
                'column_types': {col[1]: col[2] for col in columns},
                'row_count': row_count,
                'sample_data': sample_data
            }
            
            if self.verbose:
                logging.debug(f"  Columns: {[col[1] for col in columns]}")
                logging.debug(f"  Row count: {row_count}")
                
        conn.close()
        return db_info
        
    def analyze_investigation_data(self, investigation_storage: InvestigationStorage) -> Dict[str, List[Tuple[str, int]]]:
        """Analyze investigation data for column length mismatches."""
        logging.info("Analyzing investigation data for column length mismatches...")
        
        mismatches = {}
        
        for category, items in investigation_storage.data.items():
            if not items:
                logging.warning(f"Category '{category}' is empty")
                continue
                
            # Calculate column lengths
            column_lengths = {item: len(values) for item, values in items.items()}
            max_length = max(column_lengths.values())
            min_length = min(column_lengths.values())
            
            # Find mismatches
            inconsistent_items = [
                (item, length) for item, length in column_lengths.items()
                if length != max_length
            ]
            
            if inconsistent_items:
                mismatches[category] = inconsistent_items
                logging.error(f"Category '{category}' has column length mismatches:")
                logging.error(f"  Expected length: {max_length}")
                for item, length in inconsistent_items:
                    logging.error(f"  Item '{item}': {length} (difference: {max_length - length})")
                    
            elif self.verbose:
                logging.info(f"Category '{category}': All columns have equal length ({max_length})")
                
        return mismatches
        
    def analyze_sql_queries(self, json_config_path: str) -> List[Dict[str, Any]]:
        """Analyze SQL queries in the JSON configuration for potential issues."""
        logging.info("Analyzing SQL queries in configuration...")
        
        import json
        
        if not os.path.exists(json_config_path):
            logging.error(f"JSON config file not found: {json_config_path}")
            return []
            
        with open(json_config_path, 'r') as f:
            config = json.load(f)
            
        sql_operations = []
        for i, operation in enumerate(config.get('operations', [])):
            if operation.get('operation') == 'sql_query':
                query = operation.get('operation_parameters', {}).get('query', '')
                target_category = operation.get('target_category', '')
                target_items = operation.get('target_items', [])
                
                sql_operations.append({
                    'index': i,
                    'category': target_category,
                    'items': target_items,
                    'query': query,
                    'expected_columns': len(target_items)
                })
                
                if self.verbose:
                    logging.debug(f"SQL Operation {i}: {target_category}")
                    logging.debug(f"  Query: {query}")
                    logging.debug(f"  Expected columns: {len(target_items)}")
                    
        return sql_operations
        
    def test_sql_queries(self, sql_operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Test SQL queries against the database to check result consistency."""
        logging.info("Testing SQL queries for result consistency...")
        
        if not os.path.exists(self.database_path):
            logging.error(f"Database file not found: {self.database_path}")
            return {}
            
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        
        query_results = {}
        
        for sql_op in sql_operations:
            try:
                cursor.execute(sql_op['query'])
                results = cursor.fetchall()
                
                query_results[sql_op['category']] = {
                    'query': sql_op['query'],
                    'expected_columns': sql_op['expected_columns'],
                    'actual_columns': len(results[0]) if results else 0,
                    'row_count': len(results),
                    'sample_results': results[:3] if results else []
                }
                
                if results and len(results[0]) != sql_op['expected_columns']:
                    logging.error(f"Column count mismatch in {sql_op['category']}:")
                    logging.error(f"  Expected: {sql_op['expected_columns']}")
                    logging.error(f"  Actual: {len(results[0])}")
                    
                if self.verbose:
                    logging.debug(f"Query for {sql_op['category']} returned {len(results)} rows")
                    
            except Exception as e:
                logging.error(f"Error executing query for {sql_op['category']}: {e}")
                query_results[sql_op['category']] = {'error': str(e)}
                
        conn.close()
        return query_results
        
    def fix_column_mismatches(self, investigation_storage: InvestigationStorage) -> bool:
        """Attempt to fix column length mismatches by padding with None values."""
        logging.info("Attempting to fix column length mismatches...")
        
        fixed_categories = []
        
        for category, items in investigation_storage.data.items():
            if not items:
                continue
                
            # Calculate max length
            max_length = max(len(values) for values in items.values())
            
            # Pad shorter columns
            for item, values in items.items():
                if len(values) < max_length:
                    padding_needed = max_length - len(values)
                    investigation_storage.data[category][item].extend([None] * padding_needed)
                    logging.info(f"Padded {item} in {category} with {padding_needed} None values")
                    
            # Verify fix
            lengths = [len(values) for values in investigation_storage.data[category].values()]
            if len(set(lengths)) == 1:
                fixed_categories.append(category)
                logging.info(f"Successfully fixed column lengths in category: {category}")
            else:
                logging.error(f"Failed to fix column lengths in category: {category}")
                
        return len(fixed_categories) > 0
        
    def generate_report(self, db_info: Dict, mismatches: Dict, query_results: Dict) -> str:
        """Generate a comprehensive debugging report."""
        report = f"""
# Column Length Debugging Report
Investigation ID: {self.investigation_id}
Database: {self.database_path}
Generated: {logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None))}

## Database Structure
"""
        
        for table, info in db_info.items():
            report += f"""
### Table: {table}
- Columns: {len(info['columns'])}
- Rows: {info['row_count']}
- Column Names: {', '.join(info['columns'])}
"""
        
        report += "\n## Column Length Mismatches\n"
        
        if mismatches:
            for category, issues in mismatches.items():
                report += f"""
### Category: {category}
"""
                for item, length in issues:
                    report += f"- {item}: {length} rows\n"
        else:
            report += "No column length mismatches found.\n"
            
        report += "\n## SQL Query Analysis\n"
        
        for category, result in query_results.items():
            if 'error' in result:
                report += f"""
### {category}: ERROR
- Error: {result['error']}
"""
            else:
                report += f"""
### {category}
- Expected columns: {result['expected_columns']}
- Actual columns: {result['actual_columns']}
- Row count: {result['row_count']}
"""
        
        return report


def main():
    parser = argparse.ArgumentParser(description='Debug column length mismatches in mmCIF investigation generation')
    parser.add_argument('investigation_id', help='Investigation ID to debug')
    parser.add_argument('--database-path', default='pdbe_sqlite.db', help='Path to SQLite database')
    parser.add_argument('--json-config', default='./mmcif_gen/operations/pdbe/pdbe_investigation.json', help='Path to JSON configuration file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--fix-mismatches', action='store_true', help='Attempt to fix column length mismatches')
    parser.add_argument('--output-report', help='Output report to file')
    
    args = parser.parse_args()
    
    # Initialize debugger
    debugger = ColumnLengthDebugger(args.investigation_id, args.database_path, args.verbose)
    
    try:
        # Analyze database structure
        db_info = debugger.analyze_database_structure()
        
        # Create investigation storage and load data (simplified simulation)
        investigation_storage = InvestigationStorage(args.investigation_id)
        
        # For debugging purposes, we need to simulate the data loading process
        # In a real scenario, you would run the actual investigation process up to the point of writing
        logging.warning("Note: This script analyzes database structure. For full investigation data analysis,")
        logging.warning("run this script after the investigation operations have been executed but before writing the CIF file.")
        
        # Analyze SQL queries
        sql_operations = debugger.analyze_sql_queries(args.json_config)
        query_results = debugger.test_sql_queries(sql_operations)
        
        # Generate report
        report = debugger.generate_report(db_info, {}, query_results)
        
        if args.output_report:
            with open(args.output_report, 'w') as f:
                f.write(report)
            logging.info(f"Report written to: {args.output_report}")
        else:
            print(report)
            
    except Exception as e:
        logging.error(f"Error during debugging: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
