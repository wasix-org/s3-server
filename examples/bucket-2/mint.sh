docker run \
    -e "SERVER_ENDPOINT=localhost:8080"   \
    -e "ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" -e "SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    --network host \
    minio/mint:latest
