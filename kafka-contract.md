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