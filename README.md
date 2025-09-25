# Dragonfly

# Requirements to Run
1. VScode
2. Docker
3. DBeaver (Optional)

# Step 1
Clone the repository to your VSCode and make sure you have the latest version of Python Installed

# Step 2
Download the rabbitmq:4.1.3-management image and run it
```bash

docker pull rabbitmq:4.1.3-management
```
```bash 
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.1.3-management
```
You can then view the rabbitmq managment console on ```http://localhost:15672```

Default login credentials:
Username: ```guest```
Password: ```guest```

# Step 3
Download the CockroachDB:Latest Image and run it
```bash
docker pull cockroachdb/cockroach:latest

```
Create your docker network
```bash
docker network create dbnetwork
```
Create your nodes
```bash

docker run -d --name=node1 --network=dbnetwork -p 26257:26257 -p 8080:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3

docker run -d --name=node2 --network=dbnetwork -p 26258:26257 -p 8081:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3

docker run -d --name=node3 --network=dbnetwork -p 26259:26257 -p 8082:8080 cockroachdb/cockroach:latest start --insecure --join node1,node2,node3

```

Initialize the Node cluster

```bash 

docker exec -it node1 cockroach init --insecure --host=node1:26257
```


#Step 4
Create 4 terminals in VScode and each command:

Create 2 Publisher instances
```bash
python Publisher.py
```
```bash
python Publisher.py
```
Create the East and West Consumers
```bash
python East.py
```
```bash
python West.py
```

Try it out!
