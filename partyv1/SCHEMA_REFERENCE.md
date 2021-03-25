Notes: For this schema reference to work, follow below steps:
1. Register individual types for the event payload, including PlaceOrderEvent and OrderDeliveryEvent
   For example:
   ```
   curl --location --request POST 'http://localhost:8081/subjects/xiaoyf.demo.avrokafka.model.PlaceOrderEvent/versions' \
   --header 'Content-Type: application/json' \
   --data-raw '{
   "schema": "{\"type\":\"record\",\"name\":\"PlaceOrderEvent\",\"namespace\":\"xiaoyf.demo.avrokafka.model\",\"fields\":[{\"name\":\"orderId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"productName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"quantity\",\"type\":\"long\"}]}"
   }'
   ```
   Once, registered, you get a version number, can be seen from:
   `http://localhost:8081/subjects/xiaoyf.demo.avrokafka.model.OrderDeliveryEvent/versions`
2. Register OrderEvent type by using schema references
   ```
   curl --location --request POST 'http://localhost:8081/subjects/order-events-value/versions' \
   --header 'Content-Type: application/json' \
   --data-raw '{
   "schema": "{\"type\":\"record\",\"name\": \"OrderEvent\",\"namespace\": \"xiaoyf.demo.avrokafka.model\",\"fields\": [{\"name\" : \"eventId\",            \"type\" : \"string\"}, { \"name\" : \"payload\", \"type\" : [\"PlaceOrderEvent\", \"OrderDeliveryEvent\" ]}]}",
   "references" : [
   {
   "name": "PlaceOrderEvent",
   "subject":  "xiaoyf.demo.avrokafka.model.PlaceOrderEvent",
   "version": 1
   },
   {
   "name": "OrderDeliveryEvent",
   "subject":  "xiaoyf.demo.avrokafka.model.OrderDeliveryEvent",
   "version": 1
   }
   ]
   }'
   ```
   where within the "references" section, `name` is the type name used in above schema, `subject` is the subject name being used for registration in previous step.

3. In you publisher, configure these two values:
    ```
    auto.register.schemas=false
    use.latest.version=true
    ```
    such that it does not attempt to register schema, also lookup the latest version of schema for subjects.