# Spark Cluster
Run Spark in cluster mode locally using docker containers.

### Commands
|               Command               |           Use           |
|:-----------------------------------:|:-----------------------:|
|            `make build`             |    For initial setup    |
|              `make up`              |  Start all containers   |
|             `make down`             |   Stop all containers   |
| `make shell container=spark-master` | Open spark-master shell |
|   `make submit app=word_count.py`   |   Submit app to spark   |


### URLs
|          URL           |   Use    |
|:----------------------:|:--------:|
| http://localhost:4040  | Spark UI |
| http://localhost:9090  |  Master  |
| http://localhost:8081  | Worker 1 |
| http://localhost:8082  | Worker 2 |
| http://localhost:8083  | Worker 3 |
| http://localhost:18080 | History  |
