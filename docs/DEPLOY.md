# Deploying

## Using Docker

The base directory contains a `Dockerfile` file which is used to build the application in stages and produce a relatively small final image.

On the build host:

```shell
docker build -t chishiki-app .
docker image rm 192.168.1.4:5000/chishiki
docker image tag chishiki-app 192.168.1.4:5000/chishiki
docker push 192.168.1.4:5000/chishiki
```

On the server, with a production version of a `docker-compose.yml` file:

```shell
docker compose down
docker compose up --build -d
```
