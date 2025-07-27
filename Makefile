.PHONY: install airflow-standalone test lint

        install:
	pip install -r requirements.txt

        airflow-standalone:
	airflow db init
	airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com || true
	airflow standalone

        test:
	pytest -q --cov=src --cov-report=term-missing
