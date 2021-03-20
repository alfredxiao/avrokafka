Demonstrates 
* Generic Producer/Consumer
  * `TransactionPublisher0V1`
  * `TransactionListener0V1`
* Specific Producer/Consumer
  * All other publishers without `0` in their class names
  * All other listeners without `0` in their class names
* Interaction between producer/consumer and Schema Registry
* Turn on/off of schema auto register
* the `CharSequence` vs `String` field from generated class and the automatically changed schema 
    * When enable auto register, `String` works
    * When disable auto register, `String` fails, only `CharSequence` works
    * When disable auto register, schema with `avro.java.string` and `String` work
* Multitype using Union 
* Multitype using NameStrategy
* Multitype using Union+Reference
* Custom Serdes to handle unknown types
    * return null with unknown class (for both NameStrategy or Union approach)
    * use header to do filtering

How to Run
* To Generate model classes from Avro schema
  * `./gradlew generateAvroJava`
* To Run an individual class's main (there are several classes that have a `main` method)
  * Gradle does not seem to have good support on this 
  * Better do it via your IDE e.g. IntelliJ