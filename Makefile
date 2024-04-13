build:
	docker compose build

up:
	docker compose up

down:
	docker compose down --remove-orphans

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)
