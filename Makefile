venv:
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip

req:
	pip install -r requirements.txt

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

psql:
	docker exec -it airflow-crm-etl-postgres-1 psql -U airflow -d airflow

bash-web:
	docker exec -it airflow-crm-etl-airflow-webserver-1 bash

