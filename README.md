## go-pubsub
Scalable Publisher Subscriber
You can use this package to publish messages to multiple topics. Multiple subscribers subscribed to the topic will receive the message on a channel which can be used for various application.
Its extensible as a package import.

### Components
- Publisher
    - Simple interface for application to publish a message on any `topic`
- Broker
    - publisher sends the message to the broker, broker manages the connections with all the consumers
    - stores the configuration between consumers and each topic the consumers are subscribed to.
    - sends the message for the topic to the corresponding subscribed consumers
    - closes the streams in case consumers go down or unsubscribes.
- consumer
    - multiple consumers can run concurrently
    - can subscribe to multiple topics at a time
    - receives the message and sends the data to a channel (per consumer) which can be utilized by the client.

### Working
TBD

### Enhancements
- Healthchecks for consumers
- HA for publisher and broker
- Scale-up nos of brokers
- Sharding of topics/consumers between the brokers
