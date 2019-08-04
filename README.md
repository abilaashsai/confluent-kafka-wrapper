### Confluent-kafka-wrapper

It uses [KSQL](https://www.confluent.io/product/ksql/) to read and write into topic and directly consume the events through [Confluent HTTP Connector](https://www.confluent.io/connector/kafka-connect-http)

## Setup
1. Download and install [confluent platform](https://www.confluent.io/product/confluent-platform/)
2. ./gradlew build
3. ./gradlew bootRun

## Usage

### Create Namespace
1. curl -X POST -H "Content-Type:application/json" -d '{"id":"<NAMESPACE_NAME>", "description":"description"}' http://localhost:8080/namespace
  
### Publish to Event
2. curl -X POST -H "Content-Type:application/json" -d '{"data":<DATA>}' http://localhost:8080/<NAMESPACE_NAME>/event/<EVENT_NAME>
  
### Subscribe to Event
3. curl -X POST -H "Content-Type:application/json" -d '{ "name": "<SUBSCRIPTION_NAME>", "namespace":"<NAMESPACE>", "filter": ["<filter_1>","<filter_2>"], "notification": {"url": "https://cf1b7e50.ngrok.io"} }' http://localhost:8080/subscription

### Dev usage

1. curl -X GET http://localhost:8080/listtopics
