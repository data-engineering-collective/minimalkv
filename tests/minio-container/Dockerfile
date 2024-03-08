FROM minio/minio
COPY scripts/startup.sh /startup.sh
COPY policies /policies
COPY files /files
ENTRYPOINT ["/startup.sh"]
