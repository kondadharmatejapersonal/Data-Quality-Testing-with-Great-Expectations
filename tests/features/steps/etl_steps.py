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

@given('the ETL process is ready to run')
def step_impl(context):
    context.test_name = "ETL Process Execution"
    context.steps = []
    context.attachments = []
    
    # Initialize the database
    init_db()
    
    context.steps.append({
        'name': 'Preparing ETL process',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })

@when('I execute the ETL process')
def step_impl(context):
    context.steps.append({
        'name': 'Running ETL process',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    run()
    context.steps[-1]['stop'] = datetime.now().isoformat()

@then('the dim_customer table should be created')
def step_impl(context):
    context.steps.append({
        'name': 'Verifying table creation',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_customer'")
    table_exists = cursor.fetchone() is not None
    
    context.attachments.append({
        'name': 'Table Existence Check',
        'type': 'text',
        'content': f"dim_customer table exists: {table_exists}"
    })
    
    assert table_exists, "dim_customer table should exist"
    conn.close()

@then('the table should contain data')
def step_impl(context):
    context.steps.append({
        'name': 'Checking data presence',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM dim_customer")
    count = cursor.fetchone()[0]
    
    context.attachments.append({
        'name': 'Row Count',
        'type': 'text',
        'content': str(count)
    })
    
    assert count > 0, "dim_customer table should contain data"
    conn.close()

@then('there should be no null values in required fields')
def step_impl(context):
    context.steps.append({
        'name': 'Checking null values',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM dim_customer 
        WHERE customer_id IS NULL 
        OR state_code IS NULL 
        OR city IS NULL
    """)
    null_count = cursor.fetchone()[0]
    
    context.attachments.append({
        'name': 'Null Values Count',
        'type': 'text',
        'content': str(null_count)
    })
    
    assert null_count == 0, "There should be no null values in required fields"
    conn.close()

@given('the ETL process has completed')
def step_impl(context):
    context.test_name = "Data Quality Validation"
    context.steps = []
    context.attachments = []
    context.steps.append({
        'name': 'ETL process completed',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })

@when('I check the data quality')
def step_impl(context):
    context.steps.append({
        'name': 'Running data quality checks',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })

@then('all state codes should be valid')
def step_impl(context):
    context.steps.append({
        'name': 'Validating state codes',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM dim_customer dc
        LEFT JOIN base_state bs ON dc.state_code = bs.state_code
        WHERE bs.state_code IS NULL
    """)
    invalid_state_count = cursor.fetchone()[0]
    
    context.attachments.append({
        'name': 'Invalid State Codes Count',
        'type': 'text',
        'content': str(invalid_state_count)
    })
    
    assert invalid_state_count == 0, "All state codes should be valid"
    conn.close()

@then('there should be no null values in customer data')
def step_impl(context):
    context.steps.append({
        'name': 'Checking customer data nulls',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN state_code IS NULL THEN 1 ELSE 0 END) as null_state_codes,
            SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as null_cities
        FROM dim_customer
    """)
    quality_metrics = cursor.fetchone()
    
    context.attachments.append({
        'name': 'Data Quality Metrics',
        'type': 'text',
        'content': f"Total rows: {quality_metrics[0]}\n"
                  f"Null state codes: {quality_metrics[1]}\n"
                  f"Null cities: {quality_metrics[2]}"
    })
    
    assert quality_metrics[1] == 0, "There should be no null state codes"
    assert quality_metrics[2] == 0, "There should be no null cities"
    conn.close()

@then('the data should be consistent with source tables')
def step_impl(context):
    context.steps.append({
        'name': 'Checking data consistency',
        'status': 'passed',
        'start': datetime.now().isoformat()
    })
    
    conn = sqlite3.connect('data/ecommerce.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM dim_customer dc
        LEFT JOIN raw_customer rc ON dc.customer_id = rc.customer_id
        WHERE rc.customer_id IS NULL
    """)
    missing_customers = cursor.fetchone()[0]
    
    context.attachments.append({
        'name': 'Data Consistency Check',
        'type': 'text',
        'content': f"Customers missing from source: {missing_customers}"
    })
    
    assert missing_customers == 0, "All customers should exist in source table"
    conn.close()

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