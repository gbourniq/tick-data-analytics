# Add this line at the top of your Makefile
include .env
export


python_code := analyses scripts

# One-off
setup:
	@mkdir -p ~/.dbt
	@read -p "Do you want to copy profiles.yml to ~/.dbt/ (required to connect to Snowflake)? (y/n) " answer; \
	if [ "$$answer" = "y" ]; then \
		cp profiles.yml ~/.dbt/; \
		echo "profiles.yml copied to ~/.dbt/"; \
	else \
		echo "Skipping profiles.yml copy"; \
	fi
	python3.11 -m pip install poetry==1.8.4
	python3.11 -m poetry install
	python3.11 -m poetry run dbt deps
	python3.11 -m poetry run dbt debug
	python3.11 -m poetry run dbt seed
	python3.11 -m poetry run dbt run-operation stage_external_sources --vars "ext_full_refresh: true"


# Run operations
run:
	python3.11 -m poetry run dbt build --selector es_equity_index_future

analysis:
	python3.11 -m analyses.1_es_futures_adjusted_prices
	python3.11 -m analyses.2_weekly_bar_counts
	python3.11 -m analyses.3_bar_returns_correlation
	python3.11 -m analyses.4_monthly_bar_variance_analysis
	python3.11 -m analyses.5_jarque_bera_test_bar_returns


# Development
fmt-sql:
	python3.11 -m poetry run sqlfluff format .

lint-sql:
	python3.11 -m poetry run sqlfluff lint .
	python3.11 -m poetry run yamllint .
	python3.11 -m poetry run dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions

fmt-python:
	python3.11 -m poetry run autoflake --remove-all-unused-imports --remove-unused-variables --in-place --recursive $(python_code)
	python3.11 -m poetry run isort --profile black --line-length 89 $(python_code)
	python3.11 -m poetry run black --line-length 89 --preview --enable-unstable-feature=string_processing $(python_code)
	python3.11 -m poetry run ruff check --fix $(python_code)

lint-python:
	python3.11 -m poetry run autoflake --check --quiet --recursive $(python_code)
	python3.11 -m poetry run isort --profile black --line-length 89 --check-only $(python_code)
	python3.11 -m poetry run black --line-length 89 --check $(python_code)
	python3.11 -m poetry run ruff check $(python_code)
	python3.11 -m poetry run flake8 --max-line-length 89 --max-doc-length 89 $(python_code) || true

fmt:
	$(MAKE) fmt-sql
	$(MAKE) fmt-python

lint:
	$(MAKE) lint-sql
	$(MAKE) lint-python

clean:
	python3.11 -m poetry run dbt clean
	rm -rf logs .ruff_cache .mypy_cache

# Documentation
serve-docs:
	python3.11 -m poetry run dbt docs generate
	python3.11 -m poetry run dbt docs serve
