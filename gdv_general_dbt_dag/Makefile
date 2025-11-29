.PHONY: setup test lint clean deploy help

help:
	@echo "Available commands:"
	@echo "  make setup   - Install dependencies and dbt packages"
	@echo "  make test    - Run unit tests"
	@echo "  make lint    - Run code linting"
	@echo "  make clean   - Remove temporary files"
	@echo "  make deploy  - Deploy to Cloud Composer (requires GCS_BUCKET_NAME env var)"

setup:
	pip install -r requirements.txt
	dbt deps --project-dir dbt --profiles-dir dbt

test:
	# Requires pytest. Add it to requirements.txt if not present
	pytest tests/

lint:
	# Requires flake8. Add it to requirements.txt if not present
	flake8 dags_* modules

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".DS_Store" -delete

deploy:
	@if [ -z "$(GCS_BUCKET_NAME)" ]; then \
		echo "Error: GCS_BUCKET_NAME environment variable is not set"; \
		exit 1; \
	fi
	@echo "Deploying to gs://$(GCS_BUCKET_NAME)/dags ..."
	gcloud storage rsync -r . gs://$(GCS_BUCKET_NAME)/dags \
		--exclude ".git" \
		--exclude ".github" \
		--exclude "tests" \
		--exclude "__pycache__" \
		--exclude "*.pyc" \
		--exclude ".DS_Store"
