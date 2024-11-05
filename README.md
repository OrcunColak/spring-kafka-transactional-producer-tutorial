# Read Me

The original idea is from  
https://medium.com/shoutloudz/apache-kafka-transactions-349ad4617856

# Producer

We can enable transactions by

```
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdPrefix);
```

or

```
spring.kafka.producer.transaction-id-prefix=transfer-service-${random.value}-
```

# Consumer

```
spring.kafka.consumer.isolation-level=READ_COMMITTED
```

Consumer microservices should read only committed messages, If the transaction is not successful Kafka message will not
be marked as committed and this message will not be visible to consumers

By default, its value is read_uncommited, so that is not correct in case of failed transactions

