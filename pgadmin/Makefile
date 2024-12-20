# Nome da imagem e do container
IMAGE_NAME=my-pgadmin
CONTAINER_NAME=pgadmin-container
PORT=8080

# Comando para construir a imagem
build:
	docker build -t $(IMAGE_NAME) .

# Comando para iniciar o container
start:
	docker run -d --name $(CONTAINER_NAME) -p $(PORT):80 $(IMAGE_NAME)

# Comando para parar o container
stop:
	docker stop $(CONTAINER_NAME)

# Comando para remover o container
remove:
	docker rm -f $(CONTAINER_NAME)

# Comando para reconstruir a imagem e reiniciar o container
restart: stop remove build start

# Comando para visualizar logs do container
logs:
	docker logs -f $(CONTAINER_NAME)

# Comando para acessar o shell do container
shell:
	docker exec -it $(CONTAINER_NAME) /bin/bash

# Limpeza total: remove a imagem e o container
clean: stop remove
	docker rmi $(IMAGE_NAME) || true
