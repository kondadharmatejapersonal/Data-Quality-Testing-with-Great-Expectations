Feature: ETL Process for Customer Data
    As a data engineer
    I want to ensure the ETL process works correctly
    So that we can maintain data quality

    Scenario: ETL Process Execution
        Given the ETL process is ready to run
        When I execute the ETL process
        Then the dim_customer table should be created
        And the table should contain data
        And there should be no null values in required fields

    Scenario: Data Quality Validation
        Given the ETL process has completed
        When I check the data quality
        Then all state codes should be valid
        And there should be no null values in customer data
        And the data should be consistent with source tables 