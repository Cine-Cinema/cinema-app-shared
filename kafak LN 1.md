## ✅ Lesson 1.63 – Concurrent Reservation Test Passed!

Your test results are exactly what we wanted.

| Test | Actual result | Status |
|---|---|---|
| Customer A reserves seats 1 and 2 | `CONFIRMED` | ✅ Passed |
| Customer B requests the same seats | `REJECTED` | ✅ Passed |
| Customer B receives a rejection reason | `Seat 1 is already reserved` | ✅ Passed |
| Repeat Customer A's reservation ID | Original `CONFIRMED` result | ✅ Passed |

Your backend successfully handled the two simultaneous requests:

```text
Customer A                     Customer B
    |                              |
    v                              v
Request seats 1, 2          Request seats 1, 2
    |                              |
    +--------------+---------------+
                   |
                   v
             Cinema Backend
                   |
                   v
              PostgreSQL
                   |
           +-------+-------+
           |               |
           v               v
       CONFIRMED        REJECTED
```

Your repeated-request test also confirms that **resending the same confirmed reservation returns the existing result instead of attempting another booking**.

One detail remains: your outputs confirm the API behavior, but they don't include the PostgreSQL verification or manual locking test. Those are worth completing before the final demonstration.

We can now move forward with the Kafka message contracts.

---

# 🎬 Lesson 1.64 – Kafka Integration (Part 1)
## Define Shared Kafka Message Contracts

Today, we'll prepare the communication agreement between your Cinema Side and your partner's Fan Side.

**We will not start implementing the Kafka consumer or producer yet.** First, both developers need to agree on exactly what information will be exchanged.

This will help prevent integration problems when connecting your two computers.

## 1. Why do we need Kafka contracts?

Your project has two independent Spring Boot applications.

| Component | Developed by | Responsibility |
|---|---|---|
| Cinema Backend | You | Manage screenings and confirm reservations |
| Fan Backend | Your partner | Manage customers, search movies and request reservations |

Both applications have separate databases.

Instead of directly accessing each other's databases, they exchange messages.

For example, your partner's backend might publish:

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "showtimeId": 8,
  "customerId": 101,
  "customerName": "John Silva",
  "seatNumbers": [5, 6]
}
```

Your Cinema backend receives this message and processes it using the `ReservationService` we already implemented.

But what happens if your partner sends:

```json
{
  "bookingId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "screeningId": 8,
  "userId": 101,
  "seats": [5, 6]
}
```

The information may be conceptually similar, but the property names are different.

Our `ReservationRequest` expects `reservationId`, `showtimeId`, `customerId`, `customerName`, and `seatNumbers`.

Without an agreed contract, the applications won't communicate correctly.

**A Kafka contract is an agreement about the message's structure, field names, data types, and meaning.**

---

# 2. Review our three Kafka topics

We previously agreed to keep three Kafka topics.

| Kafka topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `cinema.catalog` | Cinema Backend | Fan Backend | Synchronize movies, showtimes and availability |
| `reservation.requests` | Fan Backend | Cinema Backend | Send customer booking requests |
| `reservation.results` | Cinema Backend | Fan Backend | Return booking confirmation or rejection |

We will keep these topic names unchanged.

### Complete communication workflow

```text
              YOUR COMPUTER
               CINEMA SIDE
            ┌────────────────┐
            │ Cinema Backend │
            └───────┬────────┘
                    │
                    │ Kafka: cinema.catalog
                    │
                    ▼
              ┌───────────┐
              │   KAFKA   │
              │   BROKER  │
              └─────┬─────┘
                    │
                    ▼
              FAN BACKEND
             PARTNER'S PC
                    │
                    ▼
              Fan Frontend
                    │
                    │ Customer selects seats
                    ▼
              FAN BACKEND
                    │
                    │ reservation.requests
                    ▼
              KAFKA BROKER
                    │
                    ▼
              CINEMA BACKEND
                    │
                    │ ReservationService
                    ▼
              PostgreSQL
                    │
                    │ Confirm or reject
                    ▼
              CINEMA BACKEND
                    │
                    │ reservation.results
                    ▼
              KAFKA BROKER
                    │
                    ▼
               FAN BACKEND
                    │
                    ▼
              Fan Frontend
                    │
                    ▼
          Display Reservation Result
```

We also agreed to use **gRPC**.

Kafka will deliver catalog updates during normal operation, while the Fan backend will use gRPC `ListShowtimes` to retrieve a complete catalog snapshot during startup.

These are complementary mechanisms.

---

# 3. Where should we store the contracts?

Remember your GitHub organization:

```text
Cine-Cinema
```

You created these repositories:

```text
cinema-app-cinema-side
cinema-app-fan-side
cinema-app-shared
```

The contracts belong in:

**`cinema-app-shared`**

Why?

Because they are shared between the two applications.

They should not be defined independently in both repositories.

### Open your shared repository

In Ubuntu, run:

```bash
cd ~/Desktop/"Cine Cinema"
```

Check the folders:

```bash
ls
```

If your shared repository is already cloned, open it:

```bash
cd cinema-app-shared
```

Check its contents:

```bash
ls -la
```

You may already have a file named:

```text
kafka-contract.md
```

If so, **do not delete or blindly overwrite it**. Compare its existing structure with the contract below, because your partner might already be using it.

Also check:

```bash
git status
```

Make sure you understand any uncommitted changes before editing shared files.

---

# 4. Understand the reservation request contract

We'll start with the message your partner will send.

**Topic:**

```text
reservation.requests
```

**Producer:** Fan Backend

**Consumer:** Cinema Backend

### Message structure

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "showtimeId": 8,
  "customerId": 101,
  "customerName": "John Silva",
  "seatNumbers": [5, 6]
}
```

Notice how this matches our existing Java class:

```java
public class ReservationRequest {

    private String reservationId;
    private Long showtimeId;
    private Long customerId;
    private String customerName;
    private List<Integer> seatNumbers;
}
```

This is intentional.

We can deserialize a Kafka JSON message into this DTO.

### Important rules

The reservation ID must be generated by the Fan backend and must remain the same if the same request is retried.

The Fan backend must not generate a new reservation ID when merely retrying delivery of an existing booking attempt.

It must also send the same showtime, customer and seat numbers for a retry.

**The Cinema backend calculates the total price.** We deliberately don't include `totalAmount` in the reservation request.

---

# 5. Understand the reservation result contract

After processing the request, your Cinema backend returns the outcome.

**Topic:**

```text
reservation.results
```

**Producer:** Cinema Backend

**Consumer:** Fan Backend

### Successful booking

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "status": "CONFIRMED",
  "reason": null
}
```

### Rejected booking

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "status": "REJECTED",
  "reason": "Seat 5 is already reserved"
}
```

This matches our existing:

```java
public class ReservationResultResponse {

    private final String reservationId;
    private final ReservationStatus status;
    private final String reason;
}
```

### Why return the same reservation ID?

Imagine the Fan backend has three pending reservations.

```text
Reservation A → PENDING
Reservation B → PENDING
Reservation C → PENDING
```

The Cinema backend sends:

```json
{
  "reservationId": "RESERVATION_B_ID",
  "status": "CONFIRMED",
  "reason": null
}
```

The Fan backend knows exactly which reservation to update.

```text
Reservation A → PENDING
Reservation B → CONFIRMED
Reservation C → PENDING
```

We use the reservation ID to match the result to the original request.

Our database also supports `PAID`, but payment status updates will be implemented separately. For this lesson, `reservation.results` represents the initial booking decision: `CONFIRMED` or `REJECTED`.

---

# 6. Understand the cinema catalog contract

The third topic is:

```text
cinema.catalog
```

This topic will publish a **full snapshot of a showtime** whenever relevant information changes.

For example:

```json
{
  "showtimeId": 8,
  "movieId": 1,
  "movieTitle": "Avengers: Endgame",
  "posterUrl": "/api/movies/1/poster",
  "showDate": "2027-01-15",
  "showTime": "10:00:00",
  "endDate": "2027-01-15",
  "endTime": "13:01:00",
  "price": 1500.00,
  "totalSeats": 10,
  "seatsPerRow": 5,
  "reservedSeats": [1, 2],
  "availableSeats": 8,
  "active": true
}
```

This is a **proposed snapshot structure** based on the data our Cinema backend currently exposes. Check it against any existing `catalog.json` or `catalog.proto` in your shared repository before changing an agreed contract.

### Why include reservedSeats?

Because your partner's Fan frontend needs to know which seats are unavailable.

For example:

```text
Showtime ID: 8
Total Seats: 10
Reserved: [1, 2]
```

The Fan frontend can generate:

```text
                 SCREEN
           ────────────────

Row A     XX  XX  03  04  05

Row B     06  07  08  09  10
```

### When should Cinema publish a catalog update?

Eventually, we'll publish an updated snapshot when:

- A showtime is created or edited.
- A showtime is activated or deactivated.
- A reservation is confirmed, changing seat availability.
- Relevant movie information or its poster changes.

We must be careful about ordering and committed database state. We'll implement publishing after successful database commits rather than sending information that might subsequently be rolled back.

### Important detail about posterUrl

Your partner cannot use `localhost:8082` to access the poster on your computer.

For now, the snapshot uses a relative URL:

```text
/api/movies/1/poster
```

Later, the Fan backend/frontend can combine that path with a configured Cinema backend base URL that is reachable over the LAN.

This avoids hardcoding your changing IP address into every catalog message.

---

# 7. Create the shared contract document

Let's record these rules in a Markdown file.

In your shared repository, open or create:

```text
kafka-contract.md
```

If the file already contains an agreed contract, preserve it and reconcile differences with your partner rather than replacing it automatically.

Here is the document I recommend using as the basis of your agreement.

# Cine Cinema – Kafka Message Contracts

## 1. Purpose

This document defines the Kafka messages exchanged between the Cinema Backend and the Fan Backend.

Both applications are developed independently and communicate using agreed JSON message structures.

The Cinema Backend is authoritative for showtimes, ticket prices, seat availability, and reservation decisions.

## 2. Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `cinema.catalog` | Cinema Backend | Fan Backend | Showtime catalog synchronization |
| `reservation.requests` | Fan Backend | Cinema Backend | Customer reservation requests |
| `reservation.results` | Cinema Backend | Fan Backend | Reservation processing results |

**Message format:** UTF-8 encoded JSON.

Topic names and JSON property names are case-sensitive.

## 3. Reservation Request

**Topic:** `reservation.requests`

**Message key:** `reservationId`

**Producer:** Fan Backend

**Consumer:** Cinema Backend

### Example

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "showtimeId": 8,
  "customerId": 101,
  "customerName": "John Silva",
  "seatNumbers": [5, 6]
}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `reservationId` | String | Yes | UUID-formatted unique reservation identifier |
| `showtimeId` | Long | Yes | ID of the requested showtime |
| `customerId` | Long | Yes | Customer ID from Fan Backend |
| `customerName` | String | Yes | Customer display name, maximum 100 characters |
| `seatNumbers` | Array of integers | Yes | Requested seat numbers |

### Validation Rules

1. `reservationId` must be a valid UUID-formatted string.
2. `showtimeId` must identify an existing showtime.
3. `customerId` must be positive.
4. `customerName` must not be blank.
5. `seatNumbers` must contain at least one seat.
6. Every requested seat number must be positive.
7. Seat numbers must not be duplicated within the request.
8. Requested seats must exist in the selected showtime.
9. The showtime must be active and must not have started.
10. Every requested seat must be available.
11. The Cinema Backend calculates the total amount from the authoritative showtime ticket price.
12. All seats in one request are confirmed together or the entire request is rejected.

### Retry Rule

The Fan Backend must reuse the same `reservationId` when retrying delivery of the same booking attempt.

A retry must preserve the original booking information.

A new booking attempt requires a new reservation ID.

## 4. Reservation Result

**Topic:** `reservation.results`

**Message key:** `reservationId`

**Producer:** Cinema Backend

**Consumer:** Fan Backend

### Confirmed Example

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "status": "CONFIRMED",
  "reason": null
}
```

### Rejected Example

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "status": "REJECTED",
  "reason": "Seat 5 is already reserved"
}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `reservationId` | String | Yes | Matches the original reservation request |
| `status` | String | Yes | `CONFIRMED` or `REJECTED` |
| `reason` | String or null | Yes | Explanation when a reservation is rejected |

### Processing Rules

1. The Cinema Backend validates incoming reservation requests.
2. Only the Cinema Backend can confirm a reservation.
3. A confirmed reservation allocates all requested seats.
4. A rejected reservation does not allocate any seats.
5. The Fan Backend matches the response using `reservationId`.
6. A repeated booking request should not allocate seats again.
7. Reservation results must reflect committed database operations.
8. Database or messaging failures require safe retry/error handling.

**Note:** The Cinema database also supports `PAID`. Payment-status synchronization will be agreed separately before implementation.

## 5. Cinema Catalog Snapshot

**Topic:** `cinema.catalog`

**Message key:** `showtimeId`, represented as a string for the Kafka key.

**Producer:** Cinema Backend

**Consumer:** Fan Backend

Each message represents the latest known complete snapshot of one showtime.

### Proposed Example

```json
{
  "showtimeId": 8,
  "movieId": 1,
  "movieTitle": "Avengers: Endgame",
  "posterUrl": "/api/movies/1/poster",
  "showDate": "2027-01-15",
  "showTime": "10:00:00",
  "endDate": "2027-01-15",
  "endTime": "13:01:00",
  "price": 1500.00,
  "totalSeats": 10,
  "seatsPerRow": 5,
  "reservedSeats": [1, 2],
  "availableSeats": 8,
  "active": true
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `showtimeId` | Long | Unique showtime identifier |
| `movieId` | Long | Movie identifier |
| `movieTitle` | String | Movie title |
| `posterUrl` | String or null | Relative public movie poster path |
| `showDate` | String | Date in `YYYY-MM-DD` format |
| `showTime` | String | Local time in `HH:mm:ss` format |
| `endDate` | String | Calculated ending date |
| `endTime` | String | Calculated ending time |
| `price` | Decimal number | Cinema-authoritative ticket price |
| `totalSeats` | Integer | Total seating capacity |
| `seatsPerRow` | Integer | Seats in each generated row |
| `reservedSeats` | Array of integers | Confirmed or paid seat allocations |
| `availableSeats` | Integer | Total seats minus reserved seat count |
| `active` | Boolean | Whether the manager has enabled bookings |

### Catalog Rules

1. The Cinema Backend owns catalog information.
2. Catalog messages contain a complete showtime snapshot, not only changed fields.
3. The Fan Backend stores the latest received catalog state in its own database.
4. The Cinema Backend publishes changes after successful database commits.
5. Seat availability may change between catalog updates and booking requests.
6. The Cinema Backend must validate every incoming reservation against its own database.
7. An inactive showtime is not available for new reservations, even if seats are unreserved.
8. The `posterUrl` must be resolved using a Cinema Backend address reachable by the Fan application.

**Compatibility note:** Compare this proposed snapshot with any existing shared `catalog.json` and `catalog.proto` before changing their agreed field names.

## 6. gRPC Catalog Synchronization

Kafka provides catalog updates during normal operation.

The Fan Backend additionally uses the gRPC method:

```text
CatalogService.ListShowtimes
```

The Cinema Backend streams current showtime snapshots so the Fan Backend can initialize or refresh its local catalog.

The gRPC snapshot and Kafka catalog event must represent consistent business information.

The protobuf schema remains in the shared repository.

## 7. Networking

Both applications run on separate computers on the same local network.

- Cinema REST Backend: port `8082`.
- Fan REST Backend: port `8080`.
- Kafka broker address: configurable.
- Cinema gRPC endpoint: configurable.

Do not hardcode changing computer IP addresses into Java source files.

Use environment variables or resolvable local hostnames.

Each backend owns its PostgreSQL database. Database ports and credentials are not part of the Kafka message contracts.

## 8. Security and Reliability

1. Do not include passwords, JWT tokens, or database credentials in Kafka messages.
2. Do not include payment-card information in Kafka messages.
3. Use reservation IDs to correlate requests and results.
4. Preserve message ordering where required by choosing appropriate Kafka message keys.
5. Prevent double bookings using Cinema Backend validation, database transactions, locking, and uniqueness constraints.
6. Implement retry handling without creating duplicate reservations.
7. Handle failures between database commits and Kafka publishing.
8. Do not interpret a Kafka delivery acknowledgment as proof that a reservation was confirmed.

## 9. Current Implementation Status

| Component | Status |
|---|---|
| Cinema reservation entities | Implemented |
| Cinema reservation repositories | Implemented |
| Cinema ReservationService | Implemented |
| Basic concurrent reservation test | Passed |
| Confirmed-reservation retry test | Passed |
| Kafka contract documentation | This lesson |
| Kafka broker integration | Pending |
| Kafka request consumer | Pending |
| Kafka result producer | Pending |
| Live catalog producer | Pending |
| gRPC catalog service | Pending |
| Cross-computer integration | Pending |

## 10. Agreement Between Developers

Before implementing integration, both developers should confirm:

- Kafka topic names.
- JSON property names and data types.
- Reservation ID generation and retry rules.
- Booking status values.
- Catalog snapshot fields.
- Kafka message keys.
- Local network broker configuration.
- Error-handling and retry responsibilities.

Any future breaking contract change must be coordinated between the Cinema Side and Fan Side developers.

---

# 8. Understand Kafka message keys

There's an important distinction between a **Kafka topic** and a **Kafka message key**.

A topic identifies the stream of messages.

For example:

```text
reservation.requests
```

A key identifies how a message should be partitioned.

For reservation messages, we'll use:

```text
Key = reservationId
```

For catalog messages:

```text
Key = showtimeId
```

Why?

Kafka uses the key when selecting a partition, so messages with the same key can be directed to the same partition.

This helps preserve the order of related messages within that partition.

For example:

```text
reservation.results
        |
        | Key: ABC
        v
Reservation ABC → CONFIRMED
Reservation ABC → PAID (future feature)
```

Messages for the same reservation should be processed in the intended order.

**Remember:** Kafka's ordering guarantee applies within a partition, not globally across every partition.

We will configure message keys when implementing the Kafka producers.

---

# 9. Confirm the contracts with your partner

Before writing Kafka integration code, share the contract with your partner.

Both of you should agree on these specific details:

| Detail | Cinema Side | Fan Side |
|---|---|---|
| Reservation ID | Java `String`, UUID format | Generates UUID string |
| Showtime ID | Java `Long` | Sends corresponding numeric ID |
| Customer ID | Java `Long` | Sends numeric customer ID |
| Seat numbers | `List<Integer>` | Sends array of integers |
| Reservation result | `CONFIRMED` / `REJECTED` | Processes either result |
| Reservation total | Calculated by Cinema | Must not dictate the price |
| Catalog | Cinema produces snapshots | Fan consumes snapshots |

One thing to resolve before the payment UI is completed: our current `reservation.results` message doesn't include `totalAmount`. Since the Fan application needs to display what the customer owes, we'll need to agree on how that authoritative amount is retrieved or synchronized.

We should not add a field to your partner's existing implementation without coordinating the change.

---

# 10. Save the contract in GitHub

After both developers agree on the structure, save the Markdown file.

From:

```bash
~/Desktop/"Cine Cinema"/cinema-app-shared
```

Check your changes:

```bash
git diff -- kafka-contract.md
```

This displays changes to the shared contract.

Then:

```bash
git add kafka-contract.md
```

```bash
git commit -m "Document Kafka message contracts"
```

And:

```bash
git push
```

Your partner can then retrieve the updated contract.

**Important:** If the existing shared contract differs, discuss those differences first. Don't replace working shared definitions just because this lesson presents a newer example.

---

# 11. Test the message contracts

We haven't started Kafka yet, but we can already check whether our JSON structures are correct.

For example, create a temporary file named:

```text
reservation-request-test.json
```

Add:

```json
{
  "reservationId": "e55a89a1-7f39-4d16-9fb4-b933637a1013",
  "showtimeId": 8,
  "customerId": 101,
  "customerName": "John Silva",
  "seatNumbers": [5, 6]
}
```

Validate its JSON syntax:

```bash
python3 -m json.tool reservation-request-test.json
```

If the JSON is valid, Python will print the formatted content.

This checks JSON syntax, **not whether the values satisfy every application validation rule**.

To test the actual booking rules, we already have our development endpoint from Lesson 1.62.

Later, the Kafka consumer will deserialize messages using the agreed contract and call the same reservation service.

---

# 12. What happens when we connect both computers?

Your partner's backend is currently reachable at:

```text
10.20.11.179:8080
```

Your Cinema backend runs on:

```text
Port 8082
```

Because both computers are on the same LAN, we'll use one Kafka broker accessible to both.

For example:

```text
              YOUR COMPUTER
              Cinema Backend
                 :8082
                   |
                   |
                   v
             SHARED KAFKA
                 :9092
                   ^
                   |
                   |
              Fan Backend
                 :8080
              PARTNER'S PC
```

The diagram shows an intended network arrangement; port `9092` is not yet configured.

We'll need to configure the broker's listeners so both computers can actually connect.

Since your computer IP addresses sometimes change, we'll also make the broker address configurable rather than hardcoding it throughout the Java source code.

Your partner's PostgreSQL port `5043` isn't required for Kafka communication.

Each backend continues using its own database.

---

# 13. Sinhala Short Note 🇱🇰

### අද අපි කළේ මොකක්ද?

අද අපි **Kafka Message Contracts** ගැන ඉගෙනගත්තා.

අපේ project එකේ Cinema Backend එකයි Fan Backend එකයි වෙන වෙනම develop කරන නිසා, ඒ දෙක අතර data exchange කරන විදිහ කලින්ම එකඟ වෙන්න ඕනේ.

ඒ සඳහා තමයි Kafka Contracts භාවිත කරන්නේ.

**අපේ Kafka Topics තුන:**

**1. `cinema.catalog`**

Cinema Backend එකෙන් movie, showtime සහ seat availability තොරතුරු Fan Backend එකට යවනවා.

**2. `reservation.requests`**

Fan Backend එකෙන් customer booking request එක Cinema Backend එකට යවනවා.

**3. `reservation.results`**

Cinema Backend එක booking එක validate කරලා `CONFIRMED` හෝ `REJECTED` result එක Fan Backend එකට යවනවා.

### Shared Repository එක අවශ්‍ය ඇයි?

Cinema Side එකයි Fan Side එකයි එකම JSON field names සහ data types භාවිත කරන බව තහවුරු කරන්න.

උදාහරණයක්:

```text
Fan Backend sends: reservationId
Cinema Backend expects: reservationId
```

දෙපැත්තේ field names වෙනස් වුණොත් message එක නිවැරදිව process කරන්න බැරි වෙන්න පුළුවන්.

### Kafka සහ gRPC අතර වෙනස

**Kafka:** Applications දෙක අතර අලුත් events සහ updates හුවමාරු කරනවා.

**gRPC:** Fan Backend එක start වෙන විට Cinema Backend එකෙන් current showtime catalog එක ලබාගන්න භාවිත කරනවා.

මේ දෙකම භාවිත කිරීමෙන් Fan Side database එක synchronize කරගන්න පුළුවන්.

---

## ✅ Lesson 1.64 Summary

| Feature | Status |
|---|---|
| Concurrent reservation API test | ✅ Passed |
| Repeated confirmed reservation test | ✅ Passed |
| Reservation request contract | Defined for agreement |
| Reservation result contract | Defined for agreement |
| Catalog snapshot contract | Proposed for agreement |
| Shared Markdown documentation | Ready to add |
| Shared contract verification with partner | Pending |
| Kafka broker | Not configured yet |
| Kafka producer | Not implemented |
| Kafka consumer | Not implemented |
| Cross-computer Kafka integration | Pending |

### Check your understanding

**Q1.** Why do we store Kafka message contracts in `cinema-app-shared` instead of only in `cinema-app-cinema-side`?

**Q2.** Why does the Fan backend generate the reservation ID?

**Q3.** Why must the Cinema backend calculate the ticket price instead of trusting the Fan backend?

**Q4.** What is the difference between `reservation.requests` and `reservation.results`?

**Q5.** Why do we need gRPC if we already have Kafka catalog updates?

### Your task

Review the existing shared repository, compare the three message formats with your partner's implementation, and agree on the field names and types. No running Kafka broker is required for this lesson.

**Next lesson – Lesson 1.65: Set Up a Shared Kafka Broker with Docker.**

We'll configure one Kafka broker for both computers, explain `bootstrap.servers`, listeners and advertised listeners, and test connectivity before implementing Java Kafka producers and consumers.