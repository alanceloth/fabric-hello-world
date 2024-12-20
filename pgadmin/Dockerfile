# Use a imagem oficial do pgAdmin
FROM dpage/pgadmin4:latest

# Configuração de variáveis de ambiente obrigatórias
# Modifique os valores conforme necessário
ENV PGADMIN_DEFAULT_EMAIL=alan.lanceloth@gmail.com
ENV PGADMIN_DEFAULT_PASSWORD=admin

# Porta exposta pelo pgAdmin
EXPOSE 80

# Comando de inicialização
CMD ["/usr/local/bin/docker-entrypoint.sh"]
