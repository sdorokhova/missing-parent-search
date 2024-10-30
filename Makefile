run-search-partition-1:
	CAMUNDA_OPERATE_ELASTICSEARCH_URL=http://localhost:9200 \
# fix Elastic URL and provide other Elasticsearch connection parameters here, same parameter names as for Operate
	CAMUNDA_OPERATE_PARTITIONID=1 \
	CAMUNDA_OPERATE_IMPORTPOSITION=189080504 \
	   java -jar missing-parents-search-1.0.1-SNAPSHOT.jar
