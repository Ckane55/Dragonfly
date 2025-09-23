# Dragonfly
docker network create dbnetwork



docker run -d --name=node1 --network=dbnetwork -p 26257:26257 -p 8080:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3

docker run -d --name=node2 --network=dbnetwork -p 26258:26257 -p 8081:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3

docker run -d --name=node3 --network=dbnetwork -p 26259:26257 -p 8082:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3




docker exec -it node1 cockroach init --insecure --host=node1:26257
