deploy_elastic:
	docker run --name elastic -d -p 9200:9200 -p 9300:9300 -v /root/esdata1:/usr/share/elasticsearch/data -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.1

deploy_kibana:
	docker run --name kibana -d --link elastic:elasticsearch -p 5601:5601 docker.elastic.co/kibana/kibana:7.10.1
