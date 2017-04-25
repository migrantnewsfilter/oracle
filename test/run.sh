#!/bin/sh

# activate venv TODO: dockerize scripts and remove this for CI environment.
source ../venv/bin/activate
docker network create test-oracle
docker run --name test-oracle-mongo -d -p 27017:27017 --network test-oracle mongo

aws s3 cp --no-sign-request s3://migrantnews-app-db-dev-dumps/terms/2016-11-11T18:17:26Z.json - | mongoimport -h localhost --db newsfilter --collection terms

docker run --network test-oracle -e MONGO_HOST=mongodb://test-oracle-mongo:27017 migrantnewsfilter/alerts-rss

python -c 'import basic; basic.test_database_fresh()'
python -c 'import basic; basic.label_randomly(10)'

docker run --network test-oracle -e MONGO_HOST=mongodb://test-oracle-mongo:27017 migrantnewsfilter/oracle

python -c 'import basic; basic.test_predictions_present()'
python -c 'import basic; basic.test_clusters_present()'

docker stop test-oracle-mongo && docker rm test-oracle-mongo
docker network rm test-oracle

# add exit code from tests... if any are 1, exit 1!
