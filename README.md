# Data Quality Testing with Great Expectations

A comprehensive data quality testing framework using Great Expectations for e-commerce data validation.

## Project Structure

```
.
├── src/                    # Source code
│   ├── ecommerce/         # E-commerce specific code
│   └── setup/             # Setup and configuration
├── tests/                 # Test files
│   ├── features/          # BDD feature files
│   ├── step_definitions/  # Step definition files
│   ├── environment.py     # Behave environment configuration
│   └── data/             # Test data files
├── data/                  # Data files and databases
├── requirements/          # Project dependencies
│   ├── requirements.txt   # Production dependencies
│   └── requirements-dev.txt # Development dependencies
├── scripts/               # Utility scripts
├── docs/                  # Documentation
└── config/                # Configuration files
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements/requirements.txt
pip install -r requirements/requirements-dev.txt
```

## Usage

### Running the ETL Process

1. Initialize the database:
```bash
python src/ecommerce/init_db.py
```

2. Run the ETL process:
```bash
python src/ecommerce/dim_customer_etl.py
```

### Data Quality Checks

The system automatically performs data quality checks using Great Expectations:
- Validates data types
- Checks for null values
- Ensures referential integrity
- Validates state codes
- Monitors data consistency

### Viewing Test Results

Test results are available in the `allure-report` directory. To view them:
```bash
allure serve allure-results
```

## Development

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function parameters and return values
- Write docstrings for all functions and classes
- Keep functions small and focused on a single responsibility

### Adding New Features

1. Create a new branch from main:
```bash
git checkout -b feature/your-feature-name
```

2. Implement your changes
3. Add tests for new functionality
4. Update documentation
5. Create a pull request

### Database Schema Changes

When modifying the database schema:
1. Update the schema in `src/ecommerce/init_db.py`
2. Add migration scripts if needed
3. Update relevant ETL processes
4. Update tests to reflect changes

## Testing

### Running Tests

1. Run all tests:
```bash
behave tests/features
```

2. Run specific feature:
```bash
behave tests/features/your_feature.feature
```

### Test Structure

- Feature files in `tests/features/`
- Step definitions in `tests/step_definitions/`
- Environment configuration in `tests/environment.py`
- Test data in `tests/data/`

### Test Reports

Test reports are generated using Allure:
- HTML reports in `allure-report/`
- Raw results in `allure-results/`

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Pull Request Process

1. Update the README.md with details of changes
2. Ensure all tests pass
3. Update documentation as needed
4. Request review from maintainers

### Code Review Guidelines

- Reviewers should check for:
  - Code quality and style
  - Test coverage
  - Documentation updates
  - Backward compatibility
  - Performance implications

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### MIT License

Copyright (c) 2025 Dharmateja Konda

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. 