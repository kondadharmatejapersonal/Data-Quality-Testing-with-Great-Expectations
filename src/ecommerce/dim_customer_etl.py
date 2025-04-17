import sys
from pathlib import Path
import sqlite3
import great_expectations as gx

def write_non_validated_base_state(db_cursor):
    """Extract and transform state data from raw_state table."""
    db_cursor.execute(
        """
        INSERT INTO non_validated_base_state (state_id, state_code, state_name)
        SELECT
            CAST(state_id AS INTEGER) AS state_id,
            CAST(state_code AS TEXT) AS state_code,
            CAST(state_name AS TEXT) AS state_name
        FROM raw_state
        """
    )

def publish_base_state(db_cursor):
    """Load validated state data into base_state table."""
    db_cursor.execute(
        """
        INSERT INTO base_state (state_id, state_code, state_name, etl_inserted)
        SELECT
            state_id,
            state_code,
            state_name,
            datetime('now') AS etl_inserted
        FROM non_validated_base_state
        WHERE state_id not in (select state_id from base_state);
        """
    )   

def write_non_validated_base_customer(db_cursor):
    """Extract and transform customer data from raw_customer table."""
    db_cursor.execute(
        """
        INSERT INTO non_validated_base_customer (
            customer_id, zipcode, city, state_code, 
            datetime_created, datetime_updated
        )
        SELECT
            customer_id,
            zipcode,
            city,
            state_code,
            datetime_created AS datetime_created,
            datetime_updated AS datetime_updated
        FROM raw_customer;
        """
    )

def publish_base_customer(db_cursor):
    """Load validated customer data into base_customer table."""
    db_cursor.execute(
        """
        INSERT INTO base_customer (
            customer_id, zipcode, city, state_code, 
            datetime_created, datetime_updated, etl_inserted
        )
        SELECT
            customer_id,
            zipcode,
            city,
            state_code,
            datetime_created,
            datetime_updated,
            datetime('now')
        FROM non_validated_base_customer
        WHERE customer_id not in (select customer_id from base_customer);
        """
    )

def write_non_validated_dim_customer(db_cursor):
    """Transform and combine customer and state data."""
    db_cursor.execute(
        """
        INSERT INTO non_validated_dim_customer (
            customer_id, zipcode, city, state_code, 
            state_name, datetime_created, datetime_updated
        )
        SELECT DISTINCT
            c.customer_id,
            c.zipcode,
            c.city,
            c.state_code,
            s.state_name,
            c.datetime_created,
            c.datetime_updated
        FROM base_customer AS c
        INNER JOIN base_state AS s ON c.state_code = s.state_code;
        """
    )

def publish_dim_customer(db_cursor):
    """Load validated dimensional customer data."""
    db_cursor.execute(
        """
        INSERT INTO dim_customer(
            customer_id, zipcode, city, state_code, 
            state_name, datetime_created, datetime_updated, etl_inserted
        )
        SELECT
            customer_id,
            zipcode,
            city,
            state_code,
            state_name,
            datetime_created,
            datetime_updated,
            datetime('now')
        FROM non_validated_dim_customer
        where customer_id not in (select customer_id from dim_customer);
        """
    )

def audit(expectation_suite_to_check):
    """Run Great Expectations validation on the specified suite."""
    context_root_dir = Path.cwd() / "ecommerce" / "gx"
    expc_json_path = context_root_dir / "expectations" / f"{expectation_suite_to_check}.json"
    file_path = Path(expc_json_path)

    if file_path.exists():
        context = gx.get_context(context_root_dir=context_root_dir)
        validations = [{
            "batch_request": context.get_datasource("ecommerce_db")
                                  .get_asset(expectation_suite_to_check)
                                  .build_batch_request(),
            "expectation_suite_name": expectation_suite_to_check,
        }]
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", 
            validations=validations
        ).list_validation_results()
    return None 

def check_audit_failures(validation_results):
    """Check for validation failures in the audit results."""
    if not validation_results:
        return True

    results = []
    for validation_result in validation_results:
        for result in validation_result.get('results', []):
            if result.get("expectation_config", {}).get('meta', {}).get('level', 'ERROR') == 'ERROR':
                results.append(result.get('success'))
            else:
                print("================THIS IS A WARNING DQ ISSUE==================")
                print(result)
    return all(results) 

def run():
    """Main ETL process with data quality checks."""
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()

    try:
        # Extract and load state data
        write_non_validated_base_state(cursor)
        base_state_validation_result = audit('non_validated_base_state')
        if check_audit_failures(base_state_validation_result):
            publish_base_state(cursor)
        else:
            print("======== base_state DQ check failed ==========")
            print(base_state_validation_result)
            sys.exit(1)

        # Extract and load customer data
        write_non_validated_base_customer(cursor)
        base_customer_validation_result = audit('non_validated_base_customer')
        if check_audit_failures(base_customer_validation_result):
            publish_base_customer(cursor)
        else:
            print("======== base_customer DQ check failed ==========")
            print(base_customer_validation_result)
            sys.exit(1)

        # Transform and load dimensional customer data
        write_non_validated_dim_customer(cursor)
        dim_customer_validation_result = audit('non_validated_dim_customer')
        dim_customer_count_anomaly = audit('dim_customer_dt_created_count')
        
        if (check_audit_failures(dim_customer_validation_result) and 
            check_audit_failures(dim_customer_count_anomaly)):
            publish_dim_customer(cursor)
        else:
            print("======== dim_customer DQ check failed ==========")
            print(dim_customer_validation_result)
            sys.exit(1)

        # Cleanup
        cursor.execute("DELETE FROM non_validated_dim_customer;")
        cursor.execute("DELETE FROM non_validated_base_customer;")
        cursor.execute("DELETE FROM non_validated_base_state")
        conn.commit()

    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    run() 