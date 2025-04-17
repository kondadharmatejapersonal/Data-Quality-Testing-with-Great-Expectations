.PHONY: setup test clean docs

# Setup
setup:
	python -m venv venv
	venv\Scripts\activate
	pip install -r requirements/requirements.txt
	pip install -r requirements/requirements-dev.txt

# Testing
test:
	python -m pytest tests/ --alluredir=allure-results
	allure serve allure-results

# Documentation
docs:
	mkdir -p docs
	sphinx-apidoc -o docs/ src/
	cd docs && make html

# Cleanup
clean:
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .pytest_cache
	rm -rf allure-results
	rm -rf docs/_build

# Development
dev:
	python -m pip install -e .

# Linting
lint:
	flake8 src/
	black src/ tests/
	isort src/ tests/ 