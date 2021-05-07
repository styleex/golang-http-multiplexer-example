#!/usr/bin/env bash
#ab -p req.json -n 240 -c 120 -m post http://127.0.0.1:8080/
curl -vvv http://localhost:8080/ -d @req.json
