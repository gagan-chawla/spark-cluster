init:
	cp spark.env.template spark.env
	docker-compose up

up:
	docker compose up -d

down:
	docker compose down --remove-orphans

build:
	docker compose build
	make up

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./$(app)

shell:
	docker exec -it $(container) /bin/bash

pyspark:
	docker exec -it spark-master pyspark

scala:
	docker exec -it spark-master ./bin/spark-shell
