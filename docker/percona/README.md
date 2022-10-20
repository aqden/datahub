# To build and push
```
docker build -t percona:1.3 .
docker tag percona:1.3 dockdevx/percona:1.3
docker push dockdevx/percona:1.3
```