kafka-topics  --create \
    --bootstrap-server ubu-server-1:9092 \
    --topic testing-observers \
    --partitions 3 \
    --replica-placement testing-observers.json \
    --config min.insync.replicas=1