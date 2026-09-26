installed pyspark in .venv with pip install
python datagen.py 801497440
docker compose -f docker-compose.codespaces.yml up -d

docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

docker exec spark-master curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080
to check if port is actuall live and working - got response as 200, manually added ports in codespace still can't access ports 8080 and 4040. Visibility set to public and github says page not found (error 404)

docker compose -f docker-compose.codespaces.yml down