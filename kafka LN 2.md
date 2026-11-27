# 🎬 Lesson 1.65 – Kafka Integration (Part 2)
## Set Up a Shared Kafka Broker Using Docker

Today we'll set up **Apache Kafka on your Cinema Side computer** and make it accessible to your partner's Fan Side computer over your local network.

This is our first step toward connecting the two Spring Boot applications.

We'll learn what Kafka is doing, why its networking configuration matters, how to start it using Docker, and how to test communication between both computers.

**We will not modify the Cinema or Fan backend Java code today.** First, we'll make sure Kafka itself works.

---

# 1. Our current project architecture

You and your partner have two independent applications.

| Component | Cinema Side (You) | Fan Side (Partner) |
|---|---|---|
| Backend | Spring Boot 4 | Spring Boot |
| REST port | `8082` | `8080` |
| PostgreSQL | `5432` | Approximately `5043` |
| Network | Same LAN | Same LAN |
| Kafka | Not configured yet | Not configured yet |
| gRPC | Planned | Planned |

Your partner's computer currently uses:

```text
10.20.11.179
```

Your own IP address hasn't been established yet.

### Our new architecture

```text
                 YOUR COMPUTER
               CINEMA SIDE (Ubuntu)
        ┌──────────────────────────┐
        │                          │
        │   Cinema Spring Boot     │
        │        Port 8082         │
        │                          │
        │   Cinema PostgreSQL      │
        │        Port 5432         │
        │                          │
        │   Docker                 │
        │   ┌──────────────────┐   │
        │   │  KAFKA BROKER    │   │
        │   │    Port 9092     │   │
        │   └────────┬─────────┘   │
        └────────────┼─────────────┘
                     │
                     │ LOCAL NETWORK
                     │
        ┌────────────┼─────────────┐
        │            │             │
        │            ▼             │
        │    Fan Spring Boot       │
        │       Port 8080          │
        │                          │
        │    Fan PostgreSQL        │
        │       Port 5043 (?)      │
        │                          │
        └──────────────────────────┘
               PARTNER'S COMPUTER
```

**Important:** Kafka will run only on your computer. Both backends will connect to that same broker.

Your partner does not need to run another Kafka broker.

---

# 2. Understand the Kafka broker

Think about Kafka as a message storage and delivery system.

For example:

```text
Fan Backend
     |
     | Reservation request
     v
Kafka Broker
     |
     | reservation.requests
     v
Cinema Backend
     |
     | Booking confirmation
     v
Kafka Broker
     |
     | reservation.results
     v
Fan Backend
```

Unlike a normal HTTP request, the Fan backend doesn't need to wait for the Cinema backend to respond directly to the same network connection.

Kafka stores the messages in topics.

Consumers read messages from those topics.

### Our three topics

| Topic | Message direction |
|---|---|
| `cinema.catalog` | Cinema → Fan |
| `reservation.requests` | Fan → Cinema |
| `reservation.results` | Cinema → Fan |

We'll create these exact topics today.

We will use **Apache Kafka 4.3.2**, an official release published on October 23, 2026, and pin that version rather than using the changing `latest` tag. [kafka.apache.org](https://kafka.apache.org/community/downloads/?utm_source=chatgpt.com)

Kafka 4.x uses **KRaft** rather than ZooKeeper. For our small project, one Docker container can act as both broker and controller. [HowToDoInJava](https://howtodoinjava.com/kafka/kafka-cluster-setup-using-docker-compose/?utm_source=chatgpt.com)

---

# 3. Understand the networking problem

Before writing Docker configuration, we need to understand an important Kafka concept.

### Why can't we simply use localhost?

On your computer:

```text
localhost:9092
```

means your computer.

But on your partner's computer:

```text
localhost:9092
```

means **her computer**.

It doesn't point to your Kafka broker.

Therefore, your partner needs an address that identifies your computer over the LAN.

For example:

```text
YOUR_CINEMA_IP:9092
```

There's another complication.

**Kafka clients connect to a bootstrap address, then receive broker addresses from Kafka's metadata. Those advertised addresses must also be reachable.**

That's why correctly configuring `advertised.listeners` is essential when Kafka runs inside Docker. [hub.docker.com](https://hub.docker.com/r/apache/kafka?utm_source=chatgpt.com)

We'll configure the broker so both Spring Boot applications can use it from their respective computers.

---

# 4. Find your Cinema computer's IP address

Open a terminal on **your Ubuntu computer**.

Run:

```bash
hostname
```

This displays your computer's hostname.

Next, run:

```bash
hostname -I
```

You may see several IP addresses:

```text
10.20.11.150 172.17.0.1 172.18.0.1
```

These are example addresses, not your actual addresses.

The `172.x.x.x` values might belong to Docker networks.

We want the address of the network interface that communicates with your partner.

Because we know her current IP, run:

```bash
ip -4 route get 10.20.11.179
```

You should see output containing something like:

```text
10.20.11.179 dev wlp2s0 src 10.20.11.150
```

The important part is:

```text
src 10.20.11.150
```

That would be the IP address your computer uses to reach your partner.

**Write down the actual `src` address from your output.**

We'll call it:

```text
YOUR_CINEMA_IP
```

---

# 5. What if our IP addresses change?

You previously mentioned that the network sometimes assigns different IP addresses.

We don't want to hardcode those addresses in Java.

For this lesson, we'll put the broker's LAN address in your existing root `.env` file.

That means we can change the address without modifying application source code.

Later, we can improve this using local hostnames, such as:

```text
isa-dsampath.local
```

However, `.local` names work only when both computers can resolve them through mDNS.

**We'll first establish a working IP-based connection.** After that, we can switch to a stable hostname if both computers support it.

---

# 6. Create a separate Docker Compose file for Kafka

Your Cinema project already contains a Docker Compose configuration for PostgreSQL.

We don't need to replace it.

Instead, we'll create a separate file for Kafka.

### Open your Cinema Side project root

```bash
cd ~/Desktop/"Cine Cinema"/cinema-app-cinema-side
```

Your folder should look similar to:

```text
cinema-app-cinema-side/
│
├── backend/
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

Create a new file:

**`compose.kafka.yml`**

Why use a separate file?

Because it allows us to start and stop Kafka independently of PostgreSQL.

We'll also give Kafka its own Docker Compose project name, preventing its management commands from interfering with your existing PostgreSQL container.

---

# 7. Add your Kafka host configuration

Open your existing root `.env` file.

Do not delete or overwrite its existing PostgreSQL and JWT settings.

Add one new line:

```dotenv
KAFKA_LAN_HOST=YOUR_CINEMA_IP
```

Replace `YOUR_CINEMA_IP` with the actual IP from Step 4.

For example, if your IP were `10.20.11.150`:

```dotenv
KAFKA_LAN_HOST=10.20.11.150
```

### What does this variable mean?

`KAFKA_LAN_HOST` tells Kafka which address it should advertise to clients outside Docker.

It must be an address reachable from **both computers**.

Do not use:

```dotenv
KAFKA_LAN_HOST=localhost
```

because your partner's computer would interpret `localhost` as itself.

Your `.env` should remain ignored by Git.

Verify:

```bash
git check-ignore .env
```

Expected: Git identifies `.env` as ignored.

Later, when we configure Spring Boot, we'll also use a configurable Kafka bootstrap address.

---

# 8. Write the Kafka Docker configuration

Open:

```text
compose.kafka.yml
```

Add the following configuration.

```yaml
# Separate Compose project to avoid interfering with PostgreSQL.
name: cine-cinema-kafka

services:

  kafka:

    # Use a fixed official Apache Kafka version.
    image: apache/kafka:4.3.2

    container_name: cine-kafka

    # Expose only the Kafka client listener to the host.
    ports:
      - "9092:9092"

    environment:

      # Unique ID of this Kafka node.
      KAFKA_NODE_ID: "1"

      # One container acts as both broker and controller.
      KAFKA_PROCESS_ROLES: "broker,controller"

      # Kafka listens on three separate endpoints.
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093"

      # Addresses Kafka gives to connecting clients.
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:19092,EXTERNAL://${KAFKA_LAN_HOST:?Set KAFKA_LAN_HOST in the root .env file}:9092"

      # All three listeners use plaintext for local development.
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"

      # Kafka uses INTERNAL for broker communication.
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"

      # Controller communication uses the CONTROLLER listener.
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"

      # This single controller forms the KRaft quorum.
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"

      # Single broker means replication factor must be 1.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"

      # Single-node settings for Kafka's share coordinator.
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: "1"

      # Start consumer groups without an unnecessary initial delay.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"

      # Create application topics explicitly.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

      # Store Kafka messages and metadata in the mounted volume.
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"

    volumes:
      - kafka_data:/var/lib/kafka/data

# Preserve Kafka data when the container restarts.
volumes:
  kafka_data:
```

This follows the official Kafka Docker image's KRaft configuration approach, with separate internal and external listeners adapted for your two-computer LAN setup. [hub.docker.com](https://hub.docker.com/r/apache/kafka?utm_source=chatgpt.com)

---

# 9. Understand every important configuration

This is the most important conceptual part of today's lesson.

## A. `image`

```yaml
image: apache/kafka:4.3.2
```

This tells Docker to use the official Apache Kafka image.

Docker will download the image if it isn't already available locally.

Using a fixed version helps keep the project reproducible.

## B. `KAFKA_NODE_ID`

```yaml
KAFKA_NODE_ID: "1"
```

Each Kafka node needs an identifier.

We have only one node, so its ID is `1`.

We don't need three Kafka brokers for this university project.

## C. `KAFKA_PROCESS_ROLES`

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"
```

Kafka has two important responsibilities.

A **broker** stores messages and handles producer/consumer requests.

A **controller** manages Kafka cluster metadata.

With KRaft, one node can perform both responsibilities.

For our local project:

```text
Docker Container
       |
       +── Kafka Broker
       |
       └── Kafka Controller
```

This reduces the number of containers we need.

## D. `KAFKA_LISTENERS`

```yaml
KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093"
```

This defines the network endpoints where Kafka accepts connections.

| Listener | Port | Purpose |
|---|---|---|
| INTERNAL | `19092` | Communication inside the Kafka Docker network |
| EXTERNAL | `9092` | Communication with both computers |
| CONTROLLER | `9093` | KRaft controller communication |

The empty hostname after `//` means the broker listens on the container's available network interfaces.

This is similar to binding to `0.0.0.0`.

Notice that we publish only port `9092` to your computer.

The internal and controller ports are not published to your LAN.

## E. `KAFKA_ADVERTISED_LISTENERS`

This is the most important networking setting.

```yaml
KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:19092,EXTERNAL://${KAFKA_LAN_HOST}:9092"
```

It tells clients which addresses to use to communicate with the broker.

### INTERNAL

```text
kafka:19092
```

Here, `kafka` is the Docker Compose service name.

Other containers on the same Compose network can resolve it.

We'll use this address when executing Kafka commands inside the container.

### EXTERNAL

```text
YOUR_CINEMA_IP:9092
```

This is the address both Spring Boot applications will eventually use.

For example:

```text
Cinema Backend → 10.20.11.150:9092

Fan Backend    → 10.20.11.150:9092
```

Both connect to the same Kafka broker.

### What would happen if we advertised localhost?

Suppose the broker advertised:

```text
localhost:9092
```

Your partner might initially reach the broker using your LAN IP.

But Kafka could then tell her client to connect to:

```text
localhost:9092
```

Her client would try to contact Kafka on **her own computer**.

The connection would fail.

This is why Kafka's advertised listener must contain a reachable address. [Confluent Documentation](https://docs.confluent.io/platform/current/kafka/listeners.html?utm_source=chatgpt.com)

## F. `KAFKA_CONTROLLER_QUORUM_VOTERS`

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
```

This identifies the Kafka controller.

The format is:

```text
nodeId@hostname:port
```

In our case:

```text
1@kafka:9093
```

means controller node `1`, running at the internal Docker address `kafka:9093`.

## G. Replication factor

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
```

Kafka can replicate data across multiple brokers.

But we only have one broker.

Therefore, the replication factor must be `1`.

This is suitable for local development.

However, it doesn't provide broker redundancy.

If your computer is switched off, Kafka becomes unavailable to both applications.

## H. Docker volume

```yaml
volumes:
  - kafka_data:/var/lib/kafka/data
```

Kafka stores message logs and cluster metadata on disk.

The named Docker volume preserves that information when the container is stopped or recreated.

For example:

```text
Kafka publishes message
       |
       v
Kafka data directory
       |
       v
Docker named volume
```

**Do not use `docker compose down -v` casually.** The `-v` option deletes named volumes managed by the Compose project, which would erase this broker's stored data.

---

# 10. Validate the Docker Compose configuration

Before starting Kafka, check the configuration.

From your Cinema Side project root:

```bash
docker compose -f compose.kafka.yml config
```

### What does this command do?

It reads the YAML file, resolves environment variables, and checks the Compose configuration.

It does **not** start Kafka.

Look for:

```text
EXTERNAL://YOUR_ACTUAL_IP:9092
```

For example:

```text
EXTERNAL://10.20.11.150:9092
```

If you see an error saying `KAFKA_LAN_HOST` is not set, verify that you added it to the root `.env` file and are running the command from that directory.

Do not proceed if the advertised address is incorrect.

---

# 11. Start Kafka

Now run:

```bash
docker compose -f compose.kafka.yml up -d
```

### Understand the command

`docker compose` manages applications running in containers.

`-f compose.kafka.yml` selects our Kafka configuration file.

`up` creates and starts the service.

`-d` runs it in detached mode, so your terminal remains available.

Docker may download the Kafka image on the first run.

### Check the container

```bash
docker compose -f compose.kafka.yml ps
```

Expected result should resemble:

```text
NAME         IMAGE                 STATUS       PORTS
cine-kafka   apache/kafka:4.3.2    Up           0.0.0.0:9092->9092/tcp
```

The important things are that the container is running and port `9092` is published.

### Check Kafka logs

```bash
docker compose -f compose.kafka.yml logs --tail=80 kafka
```

Look for evidence that the Kafka broker and controller started without fatal errors.

If you want to follow logs continuously:

```bash
docker compose -f compose.kafka.yml logs -f kafka
```

Press `Ctrl+C` to stop viewing logs.

That does **not** stop the Kafka container.

---

# 12. Verify Kafka is responding

Now we'll use Kafka's built-in command-line tools.

You don't need to install Kafka directly on Ubuntu because the Docker image already contains them.

Run:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --list
```

### Understand this command

`docker exec` executes a command inside an already-running container.

`cine-kafka` is the container name.

`/opt/kafka/bin/kafka-topics.sh` is Kafka's topic-management tool.

`--bootstrap-server kafka:19092` tells the tool how to connect to the broker using its internal Docker listener.

`--list` asks Kafka to display its topics.

### Expected result

At this point, the output may be empty because we haven't created any application topics.

An empty topic list without connection errors is a valid result.

If Kafka is still starting, the command may initially fail. Check the logs and repeat it once the broker is ready.

---

# 13. Create our three Kafka topics

Now we'll create the topics agreed upon in Lesson 1.64.

We will use one partition and one replica per topic for this initial project.

**Why one partition?**

It makes message ordering easier to understand while we're learning. Later, we can discuss increasing the number of partitions.

### Topic 1 – Cinema catalog

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic cinema.catalog \
  --partitions 1 \
  --replication-factor 1
```

This topic will carry showtime catalog updates from the Cinema backend to the Fan backend.

### Topic 2 – Reservation requests

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic reservation.requests \
  --partitions 1 \
  --replication-factor 1
```

This topic will carry customer reservation requests from the Fan backend to the Cinema backend.

### Topic 3 – Reservation results

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic reservation.results \
  --partitions 1 \
  --replication-factor 1
```

This topic will carry confirmation and rejection results from the Cinema backend to the Fan backend.

### Verify the topics

Run:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --list
```

Expected:

```text
cinema.catalog
reservation.requests
reservation.results
```

Kafka may also have internal topics once consumer groups begin operating. Those aren't additional application contracts.

### What does `--if-not-exists` do?

It prevents the command from failing merely because the topic already exists.

For example, if you run the topic-creation command twice, Kafka won't create two topics with the same name.

---

# 14. Test Kafka message delivery

Before connecting Spring Boot, let's verify that Kafka can actually store and deliver messages.

We'll use a temporary topic so we don't accidentally insert test messages into the real reservation topics.

### Create a test topic

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic cine.connection-test \
  --partitions 1 \
  --replication-factor 1
```

### Produce a message

Run:

```bash
echo "Hello Kafka from Cinema Side" | \
docker exec -i cine-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 \
  --topic cine.connection-test
```

### Understand the producer

A **producer** sends messages into a Kafka topic.

Here:

```text
Producer
   |
   | Hello Kafka from Cinema Side
   v
Kafka Topic: cine.connection-test
```

### Consume the message

Execute:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic cine.connection-test \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000
```

Expected output includes:

```text
Hello Kafka from Cinema Side
```

### What does `--from-beginning` mean?

It tells the console consumer to read messages from the beginning of the topic when no existing consumer-group position is being used.

`--max-messages 1` tells the tool to stop after receiving one message.

If this test succeeds, we've verified the broker's basic produce-and-consume functionality.

---

# 15. Connect your partner's computer

Now comes the important part: testing Kafka over your local network.

Your partner's backend is currently reachable at:

```text
10.20.11.179:8080
```

But **she needs to connect to Kafka on your computer**, not the other way around.

Give your partner your actual Kafka address:

```text
YOUR_CINEMA_IP:9092
```

For example, if your Cinema IP were `10.20.11.150`:

```text
10.20.11.150:9092
```

## Test A – Check the network port

### If your partner uses Ubuntu

On her computer:

```bash
nc -vz YOUR_CINEMA_IP 9092
```

Replace `YOUR_CINEMA_IP` with your real address.

If the connection works, the command should report that the TCP connection succeeded.

### If your partner uses Windows

She can open PowerShell and run:

```powershell
Test-NetConnection YOUR_CINEMA_IP -Port 9092
```

Look for:

```text
TcpTestSucceeded : True
```

**Important:** This proves TCP connectivity only. It doesn't yet prove that the Kafka metadata and message protocol work correctly.

---

## Test B – Verify Kafka metadata from your partner's computer

If your partner uses Ubuntu, she can install `kcat`, a lightweight Kafka command-line client:

```bash
sudo apt update
sudo apt install kcat
```

Then run:

```bash
kcat -b YOUR_CINEMA_IP:9092 -L
```

### What does `-L` mean?

It asks Kafka for broker and topic metadata.

This is a stronger test than checking the port because it confirms that a Kafka client can communicate with the broker.

A successful response should identify your broker and list the application topics.

It should show an externally reachable broker address.

If the metadata advertises an incorrect hostname, a client may connect initially but fail when trying to produce or consume messages.

### Test C – Send a message across the LAN

If your partner has `kcat`, she can send a message to our temporary test topic:

```bash
echo "Hello from Fan Side" | \
kcat -b YOUR_CINEMA_IP:9092 \
-t cine.connection-test \
-P
```

Now, on **your Cinema computer**, run:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic cine.connection-test \
  --from-beginning \
  --timeout-ms 10000
```

Expected output should include:

```text
Hello Kafka from Cinema Side
Hello from Fan Side
```

The messages may appear alongside other test messages if you repeat the exercise.

If you can read the message sent from her computer, **you've verified Kafka communication across both computers**.

This is an important milestone.

---

# 16. What if your partner cannot connect?

Let's examine the most common problems.

| Problem | Possible cause | What to check |
|---|---|---|
| Connection refused | Kafka isn't listening | `docker compose -f compose.kafka.yml ps` |
| Connection times out | Firewall or network restrictions | Port 9092 and LAN connectivity |
| TCP works but Kafka client fails | Incorrect advertised listener | `KAFKA_ADVERTISED_LISTENERS` |
| Works on your PC but not hers | `localhost` or Docker hostname advertised | Use your reachable LAN address |
| Worked yesterday but fails today | Your IP changed | Update `KAFKA_LAN_HOST` |
| Container exits immediately | Incorrect KRaft configuration | Docker logs |

### Check your Ubuntu firewall

Run:

```bash
sudo ufw status
```

If UFW is active and blocks port `9092`, you can allow access from your partner's current IP:

```bash
sudo ufw allow from 10.20.11.179 to any port 9092 proto tcp
```

This is more restrictive than opening the port to every source.

If her IP changes, the rule will need updating.

**Security note:** Our Kafka configuration uses `PLAINTEXT`, meaning it doesn't authenticate clients or encrypt messages. Use it only on a trusted development LAN. Do not expose port `9092` to the public internet or configure router port forwarding for it.

---

# 17. How to handle changing IP addresses

Suppose your IP changes from:

```text
10.20.11.150
```

to:

```text
10.20.11.160
```

### Option A – Update the environment variable

Open your root `.env` file.

Change:

```dotenv
KAFKA_LAN_HOST=10.20.11.160
```

Now recreate the Kafka container so the broker advertises the new address:

```bash
docker compose -f compose.kafka.yml up -d --force-recreate
```

This preserves the named Kafka volume.

Your partner must also update her Kafka bootstrap address.

**Changing `.env` alone is not enough to update an already-running Kafka broker's advertised address.**

### Option B – Use local hostnames

This is better when your computers frequently change IP addresses.

For example, if your Ubuntu hostname is:

```text
ISA-DSAMPATH
```

a possible mDNS name is:

```text
isa-dsampath.local
```

First, verify that both computers can resolve the hostname to your current LAN IP.

On Ubuntu, you can inspect the local hostname:

```bash
hostname
```

Then test resolution:

```bash
getent ahostsv4 "$(hostname).local"
```

Your partner must also verify that the name resolves correctly from her computer.

If mDNS works reliably on both machines, we can configure:

```dotenv
KAFKA_LAN_HOST=isa-dsampath.local
```

Then both Spring Boot applications can use:

```text
isa-dsampath.local:9092
```

The hostname can remain unchanged even if the IP address changes.

If mDNS doesn't work on your network, use the configurable IP-based approach for now. A DHCP reservation is another option if you control the router.

---

# 18. Managing Kafka in Docker

These commands will be useful throughout the project.

Run them from your Cinema Side project root.

| Task | Command |
|---|---|
| Start Kafka | `docker compose -f compose.kafka.yml up -d` |
| Check Kafka status | `docker compose -f compose.kafka.yml ps` |
| View logs | `docker compose -f compose.kafka.yml logs --tail=80 kafka` |
| Stop Kafka | `docker compose -f compose.kafka.yml stop` |
| Restart Kafka | `docker compose -f compose.kafka.yml restart` |
| Remove Kafka container, preserve volume | `docker compose -f compose.kafka.yml down` |

The Kafka container is managed separately from your PostgreSQL Compose project.

Don't delete the Kafka volume unless you deliberately want to erase the broker's stored topics and messages.

### Remove the temporary test topic

Once both computers have successfully tested message delivery, run:

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --delete \
  --topic cine.connection-test
```

This removes only our temporary test topic.

Your three agreed application topics remain unchanged.

---

# 19. Sinhala short note 🇱🇰

### අද අපි කළේ මොකක්ද?

අද අපි **Cinema Side computer එකේ Kafka Broker එකක් Docker භාවිතයෙන් setup කරන විදිහ** ඉගෙනගත්තා.

**Kafka Broker කියන්නේ මොකක්ද?**

Cinema Backend එකයි Fan Backend එකයි අතර messages හුවමාරු කරන්න භාවිත කරන system එක.

**අපි Kafka එක Cinema computer එකේ run කරන්නේ ඇයි?**

අපේ project එකට Kafka Broker එකක් පමණක් ප්‍රමාණවත් නිසා.

Cinema Backend එකයි Fan Backend එකයි දෙකම ඒ Broker එකට connect වෙනවා.

**Listeners කියන්නේ මොනවාද?**

Kafka එක connections accept කරන network endpoints.

**Advertised Listeners කියන්නේ මොනවාද?**

Kafka client එකක් connect වුණාට පස්සේ Broker එකට සම්බන්ධ වෙන්න භාවිත කළ යුතු address එක Kafka විසින් client එකට දැනුම් දෙනවා.

ඒ address එක වැරදි නම් network port එක open වුණත් Kafka communication fail වෙන්න පුළුවන්.

**Docker Volume එක අවශ්‍ය ඇයි?**

Kafka container එක stop කරලා නැවත start කළත් topics සහ messages තබාගන්න.

### අද workflow එක

```text
Cinema Computer
       |
       v
Docker Kafka Broker
       |
       +── cinema.catalog
       |
       +── reservation.requests
       |
       +── reservation.results
       |
       v
Local Network
       |
       v
Fan Computer
```

මේ lesson එකේ අපි Spring Boot integration කරන්නේ නැහැ.

මුලින්ම Kafka Broker එක සහ computers දෙක අතර communication එක නිවැරදිව වැඩ කරනවද කියලා test කරනවා.

---

# ✅ Lesson 1.65 – Testing Checklist

| Test | Expected result |
|---|---|
| Docker Compose configuration | Valid |
| Kafka container starts | Running |
| Kafka logs | No fatal startup errors |
| List Kafka topics | Shows all three project topics |
| Produce local test message | Successful |
| Consume local test message | Message received |
| Partner checks TCP port 9092 | Reachable |
| Partner checks Kafka metadata | Broker and topics visible |
| Partner sends Kafka test message | Cinema computer receives it |

These are tests to perform, not results that have already been verified.

### Check your understanding

**Q1.** Why do we need only one Kafka broker for this project?

**Q2.** What is the difference between `KAFKA_LISTENERS` and `KAFKA_ADVERTISED_LISTENERS`?

**Q3.** Why can't your partner use `localhost:9092` to access Kafka on your computer?

**Q4.** Why do we use a Docker volume for Kafka?

**Q5.** Why don't we need your partner's PostgreSQL port to communicate through Kafka?

### Your task

Start by finding your Cinema computer's LAN IP and configuring `compose.kafka.yml`.

Then start Kafka and create the three topics.

**Send me the outputs of these commands:**

```bash
ip -4 route get 10.20.11.179
```

```bash
docker compose -f compose.kafka.yml ps
```

```bash
docker exec cine-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --list
```

Also tell me whether your partner's computer uses Ubuntu or Windows, so we can choose the simplest cross-computer testing method.

**Next lesson – Lesson 1.66: Connect Spring Boot to Kafka.**

We'll add the Spring Kafka dependency, configure the Cinema backend to connect to our shared broker, and test receiving Kafka messages before implementing the complete reservation consumer.