from behave import given, when, then, step
import sqlite3
from pathlib import Path
import sys
import json
from datetime import datetime
from allure_behave.hooks import allure_report

# Add the parent directory to the path so we can import the ETL module
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.ecommerce.dim_customer_etl import run
from src.ecommerce.init_db import init_db

def write_allure_report(test_name, status, description, steps, attachments=None):
    """Write a simple Allure report"""
    report_dir = Path('allure-results')
    report_dir.mkdir(exist_ok=True)
    
    report = {
        'name': test_name,
        'status': status,
        'description': description,
        'steps': steps,
        'attachments': attachments or [],
        'timestamp': datetime.now().isoformat()
    }
    
    report_file = report_dir / f"{test_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

def record_step(context, step_name, operation):
    """Helper function to record step execution with proper error handling"""
    step_info = {
        'name': step_name,
        'start': datetime.now().isoformat()
    }
    
    try:
        result = operation()
        step_info.update({
            'status': 'passed',
            'stop': datetime.now().isoformat()
        })
        return result
    except Exception as e:
        step_info.update({
            'status': 'failed',
            'stop': datetime.now().isoformat(),
            'error': str(e)
        })
        context.steps.append(step_info)
        raise

@given('the ETL process is ready to run')
def step_impl(context):
    context.test_name = "ETL Process Execution"
    context.steps = []
    context.attachments = []
    
    def initialize_db():
        init_db()
        return True
    
    record_step(context, 'Preparing ETL process', initialize_db)

@when('I execute the ETL process')
def step_impl(context):
    def execute_etl():
        run()
        return True
    
    record_step(context, 'Running ETL process', execute_etl)

@then('the dim_customer table should be created')
def step_impl(context):
    def check_table_creation():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_customer'")
        table_exists = db_cursor.fetchone() is not None
        
        context.attachments.append({
            'name': 'Table Existence Check',
            'type': 'text',
            'content': f"dim_customer table exists: {table_exists}"
        })
        
        conn.close()
        if not table_exists:
            raise Exception("dim_customer table was not created")
        return table_exists
    
    record_step(context, 'Verifying table creation', check_table_creation)

@then('the table should contain data')
def step_impl(context):
    def check_data_presence():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("SELECT COUNT(*) FROM dim_customer")
        count = db_cursor.fetchone()[0]
        
        context.attachments.append({
            'name': 'Row Count',
            'type': 'text',
            'content': str(count)
        })
        
        conn.close()
        if count == 0:
            raise Exception("dim_customer table is empty")
        return count > 0
    
    record_step(context, 'Checking data presence', check_data_presence)

@then('there should be no null values in required fields')
def step_impl(context):
    def check_null_values():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM dim_customer 
            WHERE customer_id IS NULL 
            OR state_code IS NULL 
            OR city IS NULL
        """)
        null_count = db_cursor.fetchone()[0]
        
        context.attachments.append({
            'name': 'Null Values Count',
            'type': 'text',
            'content': str(null_count)
        })
        
        conn.close()
        if null_count > 0:
            raise Exception(f"Found {null_count} null values in required fields")
        return null_count == 0
    
    record_step(context, 'Checking null values', check_null_values)

@given('the ETL process has completed')
def step_impl(context):
    context.test_name = "Data Quality Validation"
    context.steps = []
    context.attachments = []
    
    def verify_etl_completion():
        # Verify that dim_customer table exists and has data
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        db_cursor.execute("SELECT COUNT(*) FROM dim_customer")
        count = db_cursor.fetchone()[0]
        conn.close()
        
        if count == 0:
            raise Exception("ETL process has not completed successfully")
        return True
    
    record_step(context, 'ETL process completed', verify_etl_completion)

@when('I check the data quality')
def step_impl(context):
    def perform_quality_check():
        # This step is a placeholder for actual quality checks
        return True
    
    record_step(context, 'Running data quality checks', perform_quality_check)

@then('all state codes should be valid')
def step_impl(context):
    def validate_state_codes():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM dim_customer dc
            LEFT JOIN base_state bs ON dc.state_code = bs.state_code
            WHERE bs.state_code IS NULL
        """)
        invalid_state_count = db_cursor.fetchone()[0]
        
        context.attachments.append({
            'name': 'Invalid State Codes Count',
            'type': 'text',
            'content': str(invalid_state_count)
        })
        
        conn.close()
        if invalid_state_count > 0:
            raise Exception(f"Found {invalid_state_count} invalid state codes")
        return invalid_state_count == 0
    
    record_step(context, 'Validating state codes', validate_state_codes)

@then('there should be no null values in customer data')
def step_impl(context):
    def check_customer_data_nulls():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN state_code IS NULL THEN 1 ELSE 0 END) as null_state_codes,
                SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as null_cities
            FROM dim_customer
        """)
        quality_metrics = db_cursor.fetchone()
        
        context.attachments.append({
            'name': 'Data Quality Metrics',
            'type': 'text',
            'content': f"Total rows: {quality_metrics[0]}\n"
                      f"Null state codes: {quality_metrics[1]}\n"
                      f"Null cities: {quality_metrics[2]}"
        })
        
        conn.close()
        if quality_metrics[1] > 0 or quality_metrics[2] > 0:
            raise Exception(f"Found {quality_metrics[1]} null state codes and {quality_metrics[2]} null cities")
        return quality_metrics[1] == 0 and quality_metrics[2] == 0
    
    record_step(context, 'Checking customer data nulls', check_customer_data_nulls)

@then('the data should be consistent with source tables')
def step_impl(context):
    def check_data_consistency():
        conn = sqlite3.connect('data/ecommerce.db')
        db_cursor = conn.cursor()
        
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM dim_customer dc
            LEFT JOIN raw_customer rc ON dc.customer_id = rc.customer_id
            WHERE rc.customer_id IS NULL
        """)
        missing_customers = db_cursor.fetchone()[0]
        
        context.attachments.append({
            'name': 'Data Consistency Check',
            'type': 'text',
            'content': f"Customers missing from source: {missing_customers}"
        })
        
        conn.close()
        if missing_customers > 0:
            raise Exception(f"Found {missing_customers} customers missing from source table")
        return missing_customers == 0
    
    record_step(context, 'Checking data consistency', check_data_consistency)

def after_scenario(context, scenario):
    """Generate report after each scenario"""
    if hasattr(context, 'test_name'):
        write_allure_report(
            test_name=context.test_name,
            status='passed' if scenario.status == 'passed' else 'failed',
            description=scenario.name,
            steps=context.steps,
            attachments=context.attachments
        ) 