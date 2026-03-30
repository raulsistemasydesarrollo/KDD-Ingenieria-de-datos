PROJECT_NAME=proyecto-big-data

.PHONY: up down build package seed batch streaming logs

up:
	docker compose up --build -d

down:
	docker compose down -v

build:
	docker compose build

package:
	docker compose exec spark-client mvn -f /opt/spark-app/pom.xml -DskipTests package

seed:
	docker compose exec hadoop bash /opt/hadoop/bootstrap/load-seed-data.sh

batch:
	docker compose exec spark-client bash /opt/spark-app/run-batch.sh

streaming:
	docker compose exec spark-client bash /opt/spark-app/run-streaming.sh

logs:
	docker compose logs -f --tail=200
