# 🎬 Lesson 1.66 – Kafka Integration (Part 3)
## Connect Your Cinema Spring Boot Backend to Kafka

Your Kafka broker is running successfully, and both computers have exchanged messages through it.

Now we'll connect your **Cinema Spring Boot application** to that same broker.

You've also confirmed that your partner agrees with your existing `README.md`. We'll continue using that agreement **without changing the message contracts or system architecture** unless you explicitly request changes.

---

## 🎯 What we'll accomplish today

By the end of this lesson, your Cinema backend should be able to receive Kafka messages and display them in its Spring Boot logs.

| Feature | Status |
|---|---|
| Shared Kafka broker | ✅ Working |
| Kafka communication between both computers | ✅ Tested |
| Three application Kafka topics | ✅ Created |
| Agreed project contract | ✅ Partner approved |
| Spring Boot Kafka dependency | Today |
| Spring Boot Kafka configuration | Today |
| Java Kafka test consumer | Today |
| Receive a Kafka message in Spring Boot | Today |
| Process actual reservation requests | Next lesson |

**Important:** We will use the existing `cine.connection-test` topic today. We won't consume real reservation requests or modify your reservation database yet.

---

# 1. Understand what we're building

Previously, your communication test looked like this:

```text
PARTNER'S COMPUTER
  10.20.11.143
        |
        | kcat producer
        | "Hello from Fan Side"
        |
        v
  KAFKA BROKER
  10.20.11.179:9092
        |
        | cine.connection-test
        |
        v
  Kafka Console Consumer
  (Your Cinema computer)
        |
        v
  Hello from Fan Side
```

That worked successfully.

However, we don't want to use the Kafka console consumer in our final application.

We want Spring Boot to receive messages automatically.

After this lesson:

```text
        PARTNER'S COMPUTER
           10.20.11.143
                 |
                 | Kafka message
                 v
        SHARED KAFKA BROKER
          10.20.11.179:9092
                 |
                 |
                 v
          CINEMA BACKEND
             Port 8082
                 |
                 v
       @KafkaListener Method
                 |
                 v
           Spring Boot Log
                 |
                 v
       "Message received!"
```

Eventually, we'll replace the test listener with a real reservation listener:

```text
Kafka reservation.requests
          |
          v
ReservationKafkaConsumer
          |
          v
ReservationService
          |
          v
PostgreSQL
          |
          v
Kafka reservation.results
```

But we'll implement that in a separate lesson.

---

# 2. What is Spring Kafka?

You already understand Spring Boot components such as:

```java
@RestController
@Service
@Repository
```

Now we're introducing another important annotation:

```java
@KafkaListener
```

Let's compare them.

### REST Controller

```java
@PostMapping("/api/showtimes")
```

Spring automatically calls the method when an HTTP request arrives.

### Kafka Listener

```java
@KafkaListener(topics = "cine.connection-test")
```

Spring automatically calls the method when its Kafka consumer receives a message.

That's the main concept.

**A REST endpoint reacts to an HTTP request. A Kafka listener reacts to a Kafka message.**

Spring Kafka provides the producer, consumer, serialization, listener and error-handling infrastructure needed to connect Spring applications to Kafka.

Spring Boot also auto-configures much of this infrastructure, so we don't have to manually construct Kafka consumers. [Home](https://docs.spring.io/spring-boot/reference/messaging/kafka.html?utm_source=chatgpt.com)

---

# 3. Open the correct project

Open IntelliJ IDEA.

Your Cinema backend is located at:

```text
~/Desktop/Cine Cinema/cinema-app-cinema-side/backend
```

From a terminal:

```bash
cd ~/Desktop/"Cine Cinema"/cinema-app-cinema-side/backend
```

Your project should contain:

```text
backend/
├── build.gradle
├── gradlew
├── settings.gradle
└── src/
    └── main/
        ├── java/
        │   └── com/
        │       └── cinemaapp/
        │           └── cinema/
        │               ├── controller/
        │               ├── dto/
        │               ├── entity/
        │               ├── repository/
        │               └── service/
        └── resources/
            └── application.properties
```

We need to modify only three areas:

1. `build.gradle` — add the Kafka dependency.
2. Configuration — provide the broker address.
3. A new Java class — receive test messages.

We won't change your existing `ReservationService`, database entities, or agreed README.

---

# 4. Add the Spring Kafka dependency

Open:

```text
backend/build.gradle
```

Find the existing:

```gradle
dependencies {
    ...
}
```

Inside that block, add:

```gradle
// Enables Spring Boot integration with Apache Kafka.
// Includes Kafka clients and Spring Kafka auto-configuration.
implementation 'org.springframework.boot:spring-boot-starter-kafka'
```

**Do not replace your existing dependencies. Add this single dependency.**

### Why use spring-boot-starter-kafka?

Because your project uses Spring Boot **4.1.2**.

Spring Boot 4 provides a dedicated Kafka starter:

```gradle
implementation 'org.springframework.boot:spring-boot-starter-kafka'
```

Spring's official documentation recommends using this starter without specifying a separate version, allowing Spring Boot to select compatible dependencies. [Home](https://docs.spring.io/spring-kafka/reference/quick-tour.html?utm_source=chatgpt.com)

### Why don't we specify a version?

Notice that we didn't write:

```gradle
implementation 'org.springframework.boot:spring-boot-starter-kafka:4.1.2'
```

Your existing Spring Boot Gradle configuration and dependency management already control dependency versions.

This helps avoid accidentally combining incompatible Spring Kafka and Spring Boot versions.

### What does this dependency provide?

It gives us the necessary Kafka infrastructure, including:

| Component | Purpose |
|---|---|
| `KafkaConsumer` | Reads messages from Kafka |
| `KafkaProducer` | Sends messages to Kafka |
| `@KafkaListener` | Receives messages using annotated methods |
| `KafkaTemplate` | Sends Kafka messages from Spring |
| Kafka serializers | Convert Java data into Kafka message data |
| Kafka deserializers | Convert Kafka message data into Java data |

Today, we only need the consumer functionality.

### Compile after adding the dependency

Run:

```bash
./gradlew compileJava
```

Gradle should download the required dependencies if they aren't cached.

Expected:

```text
BUILD SUCCESSFUL
```

If you receive a dependency-resolution error, check that the dependency is inside the `dependencies` block, your repositories include Maven Central, and Gradle is not in offline mode.

---

# 5. Configure the Kafka connection

Now we need to tell Spring Boot where Kafka is running.

Our Kafka broker is available at:

```text
10.20.11.179:9092
```

Both computers can reach this address.

However, you previously mentioned that your IP addresses sometimes change.

Therefore, we'll use an environment variable.

## Step 5.1 – Open the root .env file

Your `.env` file is located in your Cinema Side root directory:

```text
cinema-app-cinema-side/.env
```

You already use this file for database and JWT settings.

Open it:

```bash
nano ../.env
```

Add:

```dotenv
# Address used by Spring Boot to connect to Kafka
KAFKA_BOOTSTRAP_SERVERS=10.20.11.179:9092
```

Keep your existing settings.

You should also already have:

```dotenv
KAFKA_LAN_HOST=10.20.11.179
```

These two variables have different purposes.

| Variable | Used by | Purpose |
|---|---|---|
| `KAFKA_LAN_HOST` | Docker Kafka broker | Address Kafka advertises to clients |
| `KAFKA_BOOTSTRAP_SERVERS` | Spring Boot application | Address the Kafka client initially connects to |

They refer to the same broker in our current configuration.

### Why keep two variables?

Because broker configuration and application configuration are separate responsibilities.

For example, if we later move Kafka to another computer, we can update the relevant configuration without modifying Java source code.

---

## Step 5.2 – Open application.properties

Navigate to:

```text
backend/src/main/resources/application.properties
```

You previously configured:

```properties
spring.config.import=optional:file:../.env[.properties]
```

**Keep that line unchanged.**

It allows Spring Boot to read properties from your root `.env` file when you run the application from the backend directory.

At the bottom of `application.properties`, add:

```properties
# ==================================================
# APACHE KAFKA CONFIGURATION
# ==================================================

# Kafka broker address supplied by the root .env file
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS}

# Kafka messages use String keys
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# For this first lesson, receive Kafka messages as Strings
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# For a new consumer group, read existing messages from the beginning
spring.kafka.consumer.auto-offset-reset=earliest
```

Let's understand every property.

### A. bootstrap-servers

```properties
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS}
```

This tells Spring Kafka where to find the Kafka broker.

Spring resolves:

```text
${KAFKA_BOOTSTRAP_SERVERS}
```

to:

```text
10.20.11.179:9092
```

from your `.env` file.

If the broker IP changes, you can update the configuration without modifying Java code.

### B. key-deserializer

```properties
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

Kafka messages can have a key and a value.

For example:

```text
Key:   reservation-123

Value: {"reservationId":"reservation-123","status":"CONFIRMED"}
```

A deserializer converts the bytes received from Kafka into a Java object.

Here, the key becomes a Java `String`.

### C. value-deserializer

```properties
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

For our first Kafka test, messages contain plain text.

For example:

```text
Hello from Fan Side
```

The `StringDeserializer` converts the message bytes into a Java `String`.

Later, when processing reservation requests, we'll receive JSON text and convert it into our existing `ReservationRequest` DTO.

**We don't need to configure JSON deserialization yet.**

### D. auto-offset-reset

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

Kafka stores messages with offsets.

For example:

```text
cine.connection-test
       |
       +── Offset 0: Hello Kafka from Cinema Side
       |
       +── Offset 1: Hello from Fan Side
       |
       +── Offset 2: Another test message
```

For a consumer group without previously committed offsets, `earliest` tells Kafka to start reading from the oldest retained message.

This is useful because we've already sent test messages to `cine.connection-test`.

**Important:** Once a consumer group has committed offsets, Kafka normally resumes from those offsets. It doesn't reread the entire topic on every application restart.

---

---

# 6. Create your first Kafka Listener

Now comes the interesting part.

We'll create a Java class that automatically receives messages from the Kafka broker.

### Step 6.1 – Create a new package

In IntelliJ IDEA, navigate to:

```text
src/main/java/com/cinemaapp/cinema/
```

Right-click the `cinema` package.

Select:

**New → Package**

Name it:

```text
kafka
```

Your structure becomes:

```text
com.cinemaapp.cinema
│
├── config/
├── controller/
├── dto/
├── entity/
├── repository/
├── service/
│
└── kafka/                  NEW
```

### Why create a kafka package?

Because Kafka message listeners have a different responsibility from REST controllers.

Our REST controllers handle HTTP requests.

Our Kafka listeners handle messages arriving through Kafka topics.

Keeping them in a separate package makes our architecture easier to understand.

Later, this package will contain classes such as:

```text
kafka/
├── ReservationKafkaConsumer.java
├── ReservationResultProducer.java
└── CatalogKafkaProducer.java
```

These are planned classes, not files you need to create today.

---

## Step 6.2 – Create KafkaConnectionTestListener.java

Inside the new `kafka` package, create:

**`KafkaConnectionTestListener.java`**

Add the following complete code:

```java
package com.cinemaapp.cinema.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Development-only Kafka listener.
 *
 * This class verifies that the Cinema Spring Boot backend
 * can receive messages from the shared Kafka broker.
 *
 * It listens only to the temporary cine.connection-test topic.
 *
 * It does not process reservations or modify the database.
 */
@Component
@Profile("dev")
public class KafkaConnectionTestListener {

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaConnectionTestListener.class);

    /**
     * Automatically executes when a Kafka test message arrives.
     *
     * @param message The text received from Kafka.
     */
    @KafkaListener(
            topics = "cine.connection-test",
            groupId = "cinema-dev-connection-test-v1"
    )
    public void receiveMessage(String message) {

        logger.info(
                "KAFKA TEST MESSAGE RECEIVED: {}",
                message
        );
    }
}
```

Let's understand the code.

---

# 7. Understand KafkaConnectionTestListener.java

## A. `@Component`

```java
@Component
```

This tells Spring Boot to create and manage an instance of this class.

Spring automatically discovers the class because it's inside:

```text
com.cinemaapp.cinema.kafka
```

which is under your application's root package.

We don't need to manually create the object:

```java
new KafkaConnectionTestListener();
```

Spring handles that for us.

---

## B. `@Profile("dev")`

```java
@Profile("dev")
```

This means the listener is enabled only when the `dev` Spring profile is active.

Why is this useful?

Because `cine.connection-test` is only a temporary testing topic.

We don't want our final application to keep running an unnecessary test listener.

When you run:

```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```

the listener is active.

When you run the application without the `dev` profile, Spring doesn't create this listener.

**This is the same technique we used for `ReservationDevController` in Lesson 1.62.**

---

## C. Logger

```java
private static final Logger logger =
        LoggerFactory.getLogger(KafkaConnectionTestListener.class);
```

This creates a logger for our class.

You might wonder why we don't simply write:

```java
System.out.println(message);
```

Both approaches can print text.

However, Spring Boot applications usually use logging frameworks because they support timestamps, logging levels, and structured diagnostic information.

For example:

```text
2026-11-26 10:20:15 INFO KafkaConnectionTestListener
KAFKA TEST MESSAGE RECEIVED: Hello from Fan Side
```

Logging is particularly useful when multiple Kafka consumers are operating simultaneously.

---

## D. `@KafkaListener`

This is the most important annotation:

```java
@KafkaListener(
        topics = "cine.connection-test",
        groupId = "cinema-dev-connection-test-v1"
)
```

It tells Spring Kafka:

> Create a consumer that listens to the `cine.connection-test` topic as part of the specified consumer group.

Spring Boot automatically configures the listener container and supporting infrastructure. We don't need to write a manual `while` loop to poll Kafka. [Home](https://docs.spring.io/spring-boot/reference/messaging/kafka.html?utm_source=chatgpt.com)

### Understand `topics`

```java
topics = "cine.connection-test"
```

This identifies the Kafka topic our listener will read.

Remember, we're using the temporary topic from Lesson 1.65.

We are deliberately **not** listening to:

```text
reservation.requests
```

yet.

That will happen in Lesson 1.67.

---

## E. Consumer group ID

```java
groupId = "cinema-dev-connection-test-v1"
```

A consumer group identifies a collection of Kafka consumers that coordinate reading messages from a topic.

For example:

```text
Kafka Topic
    |
    v
Consumer Group: cinema-dev-connection-test-v1
    |
    v
KafkaConnectionTestListener
```

Kafka tracks the group's progress through committed offsets.

A separate consumer group can independently read the same topic.

That's why your Kafka console consumer and Spring Boot consumer can both receive the same test messages without interfering with each other.

**Why use a separate group for testing?**

Because we don't want test-consumer offsets to interfere with the real reservation consumer.

Later, our production-style consumer will have its own group ID, such as:

```text
cinema-reservation-service
```

That will be configured when we implement the real Kafka reservation listener.

---

## F. Listener method

```java
public void receiveMessage(String message)
```

Spring automatically calls this method when the consumer receives a message.

For example, when Kafka contains:

```text
Hello from Fan Side
```

Spring passes that content into:

```java
message
```

Our method executes:

```java
logger.info(
        "KAFKA TEST MESSAGE RECEIVED: {}",
        message
);
```

The `{}` placeholder is replaced with the message value.

The result appears in the Spring Boot logs.

### Important distinction

Notice that we didn't call:

```java
receiveMessage("Hello from Fan Side");
```

ourselves.

Spring Kafka calls it automatically.

That's the key concept of **event-driven programming**.

---

# 8. Do we need a KafkaConfig.java file?

Not for this simple test.

You might see tutorials that create:

```java
@Configuration
@EnableKafka
public class KafkaConfig {
    ...
}
```

and manually define:

```java
ConsumerFactory
ConcurrentKafkaListenerContainerFactory
KafkaTemplate
```

Those approaches are useful when custom configuration is needed.

However, because we're using the Spring Boot Kafka starter, **Spring Boot already provides the basic Kafka infrastructure**.

For our current requirements, configuration properties and `@KafkaListener` are sufficient. [Home](https://docs.spring.io/spring-boot/reference/messaging/kafka.html?utm_source=chatgpt.com)

We'll introduce custom configuration only when we have a specific reason.

This keeps our university project simple.

---

# 9. Compile your project

Your modified files should now be:

```text
cinema-app-cinema-side/
│
├── .env
│
├── backend/
│   │
│   ├── build.gradle                UPDATED
│   │
│   └── src/
│       └── main/
│           │
│           ├── java/
│           │   └── com/
│           │       └── cinemaapp/
│           │           └── cinema/
│           │               └── kafka/
│           │                   └── KafkaConnectionTestListener.java
│           │
│           └── resources/
│               └── application.properties   UPDATED
│
└── compose.kafka.yml               UNCHANGED
```

Now run:

```bash
./gradlew compileJava
```

Expected:

```text
BUILD SUCCESSFUL
```

If successful, we've verified that the new Java class compiles.

However, **compilation alone does not prove that Spring Boot can connect to Kafka**.

That's what we'll test next.

---

# 10. Verify Kafka is running

Before starting Spring Boot, check the broker.

From your Cinema Side project root:

```bash
cd ~/Desktop/"Cine Cinema"/cinema-app-cinema-side
```

Run:

```bash
docker compose -f compose.kafka.yml ps
```

Kafka should be running.

You can also check that the test topic still exists:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --list
```

Expected topics include:

```text
cine.connection-test
cinema.catalog
reservation.requests
reservation.results
```

**Do not delete `cine.connection-test` yet.**

We'll use it for the next tests.

---

# 11. Start Spring Boot with the development profile

Open a terminal in your backend directory:

```bash
cd ~/Desktop/"Cine Cinema"/cinema-app-cinema-side/backend
```

Run:

```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```

### What should happen?

Spring Boot starts the normal backend components:

```text
Spring Boot
    |
    +── REST Controllers
    |
    +── Services
    |
    +── JPA Repositories
    |
    +── PostgreSQL Connection
    |
    +── Spring Security
    |
    +── Kafka Consumer
```

You should see the application starting on port `8082`.

Because the `dev` profile is active, Spring also creates:

```java
KafkaConnectionTestListener
```

The Kafka consumer then attempts to connect to:

```text
10.20.11.179:9092
```

### What might appear in the logs?

You may see Kafka logs mentioning:

```text
KafkaConsumer
```

```text
cinema-dev-connection-test-v1
```

```text
Subscribed to topic(s): cine.connection-test
```

Because we configured:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

and are using a new consumer group, the listener may receive messages already stored in the test topic.

For example:

```text
KAFKA TEST MESSAGE RECEIVED: Hello Kafka from Cinema Side

KAFKA TEST MESSAGE RECEIVED: Hello from Fan Side
```

If that happens, it's a successful result.

---

# 12. Test A – Send a message from your Cinema computer

Now we'll send a new test message.

**Keep Spring Boot running in the first terminal.**

Open another terminal in your Cinema Side project root.

Run:

```bash
echo "Hello Spring Boot Kafka Listener" | \
docker exec -i cine-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 \
  --topic cine.connection-test
```

### What does this command do?

The `echo` command generates a text message.

The `|` character passes that text to the Kafka console producer.

The producer writes it to:

```text
cine.connection-test
```

Kafka stores the message and makes it available to consumers.

### Expected Spring Boot output

Return to your Spring Boot terminal.

You should see something similar to:

```text
INFO ... KafkaConnectionTestListener :
KAFKA TEST MESSAGE RECEIVED: Hello Spring Boot Kafka Listener
```

✅ If this appears, your Cinema Spring Boot application is successfully receiving Kafka messages.

This is our first Java-based Kafka integration test.

---

# 13. Test B – Send a message from your partner's computer

This test verifies the entire network path:

**Fan computer → Kafka broker → Cinema Spring Boot**

Ask your partner to open a terminal on her Ubuntu computer.

Her hostname is:

```text
ISA-IGARAVI
```

Her IP is:

```text
10.20.11.143
```

She already installed `kcat` in Lesson 1.65.

Ask her to run:

```bash
echo "Hello Cinema Spring Boot from Fan Computer" | \
kcat -b 10.20.11.179:9092 \
-t cine.connection-test \
-P
```

### Expected output on her computer

The command should complete without an error.

### Expected output on your computer

Look at the Cinema Spring Boot terminal.

You should see:

```text
KAFKA TEST MESSAGE RECEIVED: Hello Cinema Spring Boot from Fan Computer
```

If this appears, we've proven that the complete communication path works:

```text
       FAN COMPUTER
       10.20.11.143
             |
             | kcat
             v
      Kafka Producer
             |
             v
       LOCAL NETWORK
             |
             v
       CINEMA COMPUTER
       10.20.11.179
             |
             v
       Docker Kafka
          :9092
             |
             v
      cine.connection-test
             |
             v
       Spring Kafka
             |
             v
    @KafkaListener Method
             |
             v
       Spring Boot Log
```

This will be an important milestone for the project.

---

# 14. Understand why the backend doesn't need an additional port

You might wonder:

> My Cinema backend already runs on 8082. Does the Kafka consumer need another port?

No.

Your REST API accepts HTTP connections on port:

```text
8082
```

But the Kafka consumer operates differently.

It initiates a connection to the Kafka broker:

```text
10.20.11.179:9092
```

Kafka doesn't need to initiate an HTTP connection to your REST controller.

Therefore:

| Port | Purpose |
|---|---|
| `8082` | Cinema REST API |
| `9092` | Kafka broker |
| `5432` | Cinema PostgreSQL |
| `8080` | Partner's Fan REST API |
| `5433` | Partner's PostgreSQL |

We don't need to open a new inbound port specifically for the Kafka consumer.

---

# 15. Test C – Restart Spring Boot

Let's understand Kafka consumer offsets.

Stop Spring Boot using:

```text
Ctrl + C
```

Then restart:

```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```

### What should happen?

The Kafka consumer reconnects.

Because our test group already consumed and committed its previous messages, it normally resumes from its committed offsets.

Therefore, it may **not print all the old messages again**.

That's correct.

Now send another message:

```bash
echo "Kafka message after Spring Boot restart" | \
docker exec -i cine-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 \
  --topic cine.connection-test
```

Expected:

```text
KAFKA TEST MESSAGE RECEIVED: Kafka message after Spring Boot restart
```

This demonstrates that the consumer can continue receiving messages after an application restart.

**Why is that useful?**

Because the Fan backend might publish messages while your Cinema backend is temporarily offline.

Kafka can retain those messages, and a correctly configured consumer group can resume from its stored position when the backend starts again.

The exact behavior depends on retention settings, committed offsets, and whether the consumer group already exists.

---

# 16. Common errors and solutions

### Error A – Spring cannot resolve KAFKA_BOOTSTRAP_SERVERS

Example:

```text
Could not resolve placeholder 'KAFKA_BOOTSTRAP_SERVERS'
```

This usually means Spring couldn't find the Kafka address.

Check:

```text
cinema-app-cinema-side/.env
```

It should contain:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=10.20.11.179:9092
```

Also verify that `application.properties` still imports the root `.env` file and that you start Gradle from the `backend` directory.

### Error B – Kafka connection fails

Example:

```text
Connection to node -1 could not be established
```

Check that Kafka is running:

```bash
cd ~/Desktop/"Cine Cinema"/cinema-app-cinema-side

docker compose -f compose.kafka.yml ps
```

Also confirm that your LAN IP hasn't changed.

### Error C – KafkaListener import cannot be resolved

If IntelliJ highlights:

```java
import org.springframework.kafka.annotation.KafkaListener;
```

check that you added:

```gradle
implementation 'org.springframework.boot:spring-boot-starter-kafka'
```

inside the existing `dependencies` block.

Then reload the Gradle project in IntelliJ.

### Error D – Listener doesn't receive a message

Check that:

- Spring Boot is running with the `dev` profile.
- `KafkaConnectionTestListener` is under `com.cinemaapp.cinema`.
- The topic name is exactly `cine.connection-test`.
- The Kafka broker is reachable.
- You sent the message **after** starting the application.

Also inspect the Spring Boot logs for listener or consumer errors.

### Error E – REST API works, but Kafka doesn't

Your backend might still return:

```text
Cinema back-end is running
```

from `/api/hello`, even while the Kafka consumer is having connection problems.

That's because HTTP and Kafka use different network connections.

**A working REST API does not automatically prove Kafka is connected.**

You must verify actual Kafka message reception.

---

# 17. Sinhala Short Note 🇱🇰

### අද අපි කළේ මොකක්ද?

අද අපි **Spring Boot Backend එක Kafka Broker එකට connect කරන විදිහ** ඉගෙනගත්තා.

**1. Spring Kafka Dependency**

Kafka සමඟ Java Spring Boot application එක communicate කරන්න අවශ්‍ය libraries එකතු කළා.

**2. Kafka Configuration**

අපේ Kafka Broker එක තියෙන:

```text
10.20.11.179:9092
```

address එක Spring Boot application එකට configure කළා.

**3. Kafka Listener**

Kafka topic එකකට අලුත් message එකක් ආවම ඒක automatically receive කරන්න `@KafkaListener` භාවිත කළා.

**4. Consumer Group**

Kafka messages කියවීමේ progress එක track කරන්න consumer group එකක් භාවිත කළා.

**5. Development Profile**

Test listener එක `dev` profile එකේ පමණක් run වෙන විදිහට configure කළා.

### Workflow එක

```text
Fan Computer
     |
     v
Kafka Broker
     |
     v
cine.connection-test
     |
     v
Cinema Spring Boot
     |
     v
@KafkaListener
     |
     v
Java Method
     |
     v
Spring Boot Logs
```

### මේක වැදගත් ඇයි?

ඉදිරියේදී Fan Backend එකෙන් එන actual reservation requests Kafka හරහා receive කරලා අපේ `ReservationService` එකට ලබා දෙන්න අපිට පුළුවන් වෙනවා.

---

# ✅ Lesson 1.66 – Summary

| Feature | Status |
|---|---|
| Kafka broker and networking | ✅ Previously verified |
| Kafka dependency | Implement in this lesson |
| Kafka bootstrap configuration | Implement in this lesson |
| Java Kafka test listener | Implement in this lesson |
| Consumer group | Configured for testing |
| Cinema PC → Spring Boot Kafka test | Test required |
| Fan PC → Spring Boot Kafka test | Test required |
| Actual reservation processing through Kafka | Next lesson |
| Kafka result publishing | Pending |
| Catalog publishing | Pending |
| gRPC integration | Pending |

## Check your understanding

**Q1.** What is the difference between `@PostMapping` and `@KafkaListener`?

**Q2.** Why do we need a Kafka consumer group?

**Q3.** Why do we configure `spring.kafka.bootstrap-servers` instead of hardcoding the broker address in Java?

**Q4.** Why did we add `@Profile("dev")` to our test listener?

**Q5.** If the Cinema backend is temporarily stopped, can Kafka retain messages for it to consume later?

---

## Your task

Implement the three changes, start your Cinema backend with the `dev` profile, and send a test message.

**Please share these two results:**

**1. Compilation**

```bash
./gradlew compileJava
```

**2. Kafka message reception in the Spring Boot terminal**

Expected:

```text
KAFKA TEST MESSAGE RECEIVED: Hello Spring Boot Kafka Listener
```

Once that works, ask your partner to send the message from her computer and verify that your Spring Boot listener receives it.

### ⏭️ Next: Lesson 1.67 – Kafka Reservation Request Consumer

We'll connect:

```text
Fan Backend
    ↓
reservation.requests
    ↓
Cinema Kafka Consumer
    ↓
ReservationService
    ↓
PostgreSQL
```

We'll reuse your existing reservation validation and locking logic rather than creating a second booking implementation.

**Your agreed README and Kafka contract will remain unchanged.**