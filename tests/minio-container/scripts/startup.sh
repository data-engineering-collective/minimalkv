#!/bin/sh
# Start Minio in background
minio server /data --console-address ":9001" &

# Wait for Minio to start
sleep 5

# Set alias for local Minio server
mc alias set myminio http://localhost:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}

# Add policies
mc admin policy create myminio policy1 /policies/policy1.json
mc admin policy create myminio policy2 /policies/policy2.json

# Add users and attach policies
mc admin user add myminio user1 password1
mc admin policy attach myminio policy1 --user user1
mc admin user add myminio user2 password2
mc admin policy attach myminio policy2 --user user2

# Add buckets
mc mb myminio/bucket1
mc mb myminio/bucket2

# Add files to buckets
mc cp /files/file1.txt myminio/bucket1
mc cp /files/file2.txt myminio/bucket2



# Keep container running
wait