
### Simple Performance testing

#### Producer

Attributes: compression-type, linger.ms, batch, idempotent, acks, inflight.requests, ISR
> kafka-producer-perf-test --topic perf-topic-1 --num-records 500000 --throughput -1 --record-size 2358 --producer-props bootstrap.servers=ubu-server-2:9091 buffer.memory=67108864 batch.size=10000 acks=all

Note: kafka.producer:type=producer-metrics,client-id=<producer-client-id>. The bufferpool-wait-ratio attribute should be 0. If greater than 0, this means send() calls are being blocked because the buffer is full. buffer-available-bytes will show the amount of bytes available in the buffer. A value close to 0 indicates blocking is likely happening. record-queue-time-max shows the maximum time a record was kept in the buffer. This value should be close to 0.

#### Consumer
>  kafka-consumer-perf-test --broker-list ubu-server-2:9091 --topic  perf-topic-1 --messages 5000000



### JMH MicrobenchMark to generate stats

https://www.baeldung.com/java-microbenchmark-harness
