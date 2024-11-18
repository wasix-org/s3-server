#!/bin/bash

set -xe

wasmer run --net --mapdir /data:tests/data/ s3-server.wasm -- --fs-root /data --access-key access-key-id --secret-key secret-access-key --port 8080 &

cargo test --package s3-server --test rclone -- --show-output

pkill wasmer
