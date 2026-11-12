up:
	docker compose up -d

down:
	docker compose down

nuke:
	docker compose down -v

logs:
	docker compose logs -f airflow