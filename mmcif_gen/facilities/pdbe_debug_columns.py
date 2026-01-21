"""
PDBe Column Length Debugging Module

This module provides debugging functionality for the "all columns must have equal length" error
specifically for PDBe investigations. It integrates with the mmcif-gen CLI.
"""

import sys
import os
import logging
import sqlite3
from typing import Dict, List, Tuple, Any
from collections import defaultdict

from mmcif_gen.investigation_io import InvestigationStorage, SqliteReader


class PdbeColumnDebugger:
    """Debug and analyze column length mismatches in PDBe investigation data."""
    
    def __init__(self, output_file: str = None, database_path: str = "pdbe_sqlite.db", verbose: bool = False):
        self.output_file = output_file or "pdbe_debug_columns"
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
                logging.FileHandler(f'{self.output_file}.log')
            ]
        )
        
    def analyze_database_structure(self) -> Dict[str, Any]:
        """Analyze the SQLite database structure and content."""
        logging.info("Analyzing PDBe database structure...")
        
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
            
            # Get sample data for denormalized_data table
            if table_name == "denormalized_data":
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
                sample_data = cursor.fetchall()
                
                # Check entity.type distribution
                cursor.execute("SELECT type, COUNT(*) FROM denormalized_data GROUP BY type")
                type_distribution = cursor.fetchall()
                
                db_info[table_name]['type_distribution'] = type_distribution
                db_info[table_name]['sample_data'] = sample_data
            
            db_info[table_name] = {
                'columns': [col[1] for col in columns],
                'column_types': {col[1]: col[2] for col in columns},
                'row_count': row_count
            }
            
            if self.verbose:
                logging.debug(f"  Columns: {[col[1] for col in columns]}")
                logging.debug(f"  Row count: {row_count}")
                
        conn.close()
        return db_info
        
    def analyze_sql_queries(self, json_config_path: str) -> List[Dict[str, Any]]:
        """Analyze SQL queries in the PDBe JSON configuration for potential issues."""
        logging.info("Analyzing PDBe SQL queries in configuration...")
        
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
        """Test SQL queries against the PDBe database to check result consistency."""
        logging.info("Testing PDBe SQL queries for result consistency...")
        
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
                    logging.error(f"  Query: {sql_op['query']}")
                    
                if self.verbose:
                    logging.debug(f"Query for {sql_op['category']} returned {len(results)} rows")
                    
            except Exception as e:
                logging.error(f"Error executing query for {sql_op['category']}: {e}")
                logging.error(f"  Query: {sql_op['query']}")
                query_results[sql_op['category']] = {'error': str(e)}
                
        conn.close()
        return query_results
        
    def check_entity_type_consistency(self) -> Dict[str, Any]:
        """Check entity.type field consistency in the database."""
        logging.info("Checking entity.type consistency...")
        
        if not os.path.exists(self.database_path):
            logging.error(f"Database file not found: {self.database_path}")
            return {}
            
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        
        consistency_info = {}
        
        try:
            # Check entity.type values and their distribution
            cursor.execute("SELECT DISTINCT type FROM denormalized_data")
            unique_types = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT type, COUNT(*) FROM denormalized_data GROUP BY type")
            type_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Check for potential formatting issues
            cursor.execute("SELECT DISTINCT type FROM denormalized_data WHERE type LIKE '%polymer%' OR type LIKE '%non-polymer%' OR type LIKE '%water%'")
            entity_types = [row[0] for row in cursor.fetchall()]
            
            consistency_info = {
                'unique_types': unique_types,
                'type_counts': type_counts,
                'entity_types': entity_types,
                'total_records': sum(type_counts.values())
            }
            
            logging.info(f"Found entity types: {unique_types}")
            for entity_type, count in type_counts.items():
                logging.info(f"  {entity_type}: {count} records")
                
        except Exception as e:
            logging.error(f"Error checking entity.type consistency: {e}")
            consistency_info = {'error': str(e)}
            
        conn.close()
        return consistency_info
        
    def generate_report(self, db_info: Dict, query_results: Dict, entity_consistency: Dict) -> str:
        """Generate a comprehensive PDBe debugging report."""
        import datetime
        
        report = f"""
# PDBe Column Length Debugging Report
Output File: {self.output_file}
Database: {self.database_path}
Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Database Structure
"""
        
        for table, info in db_info.items():
            report += f"""
### Table: {table}
- Columns: {len(info['columns'])}
- Rows: {info['row_count']}
- Column Names: {', '.join(info['columns'])}
"""
            if 'type_distribution' in info:
                report += "- Entity Type Distribution:\n"
                for entity_type, count in info['type_distribution']:
                    report += f"  - {entity_type}: {count}\n"
        
        report += "\n## Entity Type Consistency\n"
        
        if 'error' not in entity_consistency:
            report += f"- Total Records: {entity_consistency['total_records']}\n"
            report += f"- Unique Types: {entity_consistency['unique_types']}\n"
            report += "- Type Distribution:\n"
            for entity_type, count in entity_consistency['type_counts'].items():
                report += f"  - '{entity_type}': {count} records\n"
        else:
            report += f"- Error: {entity_consistency['error']}\n"
            
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
- Status: {'✓ OK' if result['expected_columns'] == result['actual_columns'] else '✗ MISMATCH'}
"""
        
        return report


def run_pdbe_debug_columns(args):
    """Main function to run PDBe column debugging."""
    # Initialize debugger
    debugger = PdbeColumnDebugger(args.output_file, args.database_path, args.verbose)
    
    try:
        # Analyze database structure
        db_info = debugger.analyze_database_structure()
        
        # Check entity.type consistency
        entity_consistency = debugger.check_entity_type_consistency()
        
        # Analyze SQL queries
        sql_operations = debugger.analyze_sql_queries(args.json_config)
        query_results = debugger.test_sql_queries(sql_operations)
        
        # Generate report
        report = debugger.generate_report(db_info, query_results, entity_consistency)
        
        if args.output_file:
            report_file = f"{args.output_file}.md"
            with open(report_file, 'w') as f:
                f.write(report)
            logging.info(f"Report written to: {report_file}")
        else:
            print(report)
            
        # Show summary
        print(f"\n=== PDBe DEBUGGING SUMMARY ===")
        print(f"Output File: {args.output_file or 'pdbe_debug_columns'}")
        print(f"Database: {args.database_path}")
        print(f"Tables analyzed: {len(db_info)}")
        print(f"SQL operations tested: {len(sql_operations)}")
        
        # Check for issues
        issues_found = []
        for category, result in query_results.items():
            if 'error' in result:
                issues_found.append(f"SQL error in {category}")
            elif result.get('expected_columns', 0) != result.get('actual_columns', 0):
                issues_found.append(f"Column count mismatch in {category}")
                
        if 'error' in entity_consistency:
            issues_found.append("Entity type consistency check failed")
            
        if issues_found:
            print(f"Issues found: {len(issues_found)}")
            for issue in issues_found:
                print(f"  - {issue}")
        else:
            print("No issues detected in database structure or SQL queries.")
            
    except Exception as e:
        logging.error(f"Error during PDBe debugging: {e}")
        sys.exit(1)
