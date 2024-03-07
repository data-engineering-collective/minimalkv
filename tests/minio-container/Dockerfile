FROM minio/minio
COPY scripts/startup.sh /startup.sh
COPY policies /policies
COPY files /files
RUN chmod +x /startup.sh
ENTRYPOINT ["/startup.sh"]
