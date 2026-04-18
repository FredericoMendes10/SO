# KVS — Key-Value Store with Pub/Sub

A key-value store (KVS) server with real-time pub/sub notifications, written in C as an Operating Systems course project.

## Architecture

The system consists of two executables that communicate via named pipes (FIFOs):

- **Server** (`src/server/kvs`) — stores key-value pairs, processes job files using multiple worker threads, and notifies subscribed clients when values are updated or deleted.
- **Client** (`src/client/client`) — connects to the server, sends commands interactively, and receives asynchronous notifications on a dedicated thread.

### Key technical concepts
- IPC via named pipes (FIFOs)
- POSIX threads with RW-locks and mutexes
- Producer-consumer pattern with semaphores
- Hash table with chaining (26 buckets)
- Signal handling (`SIGUSR1` to disconnect all clients)
- Concurrent backups via child processes (`fork`)

## Build

```bash
make
```

## Running the Server

```bash
./src/server/kvs <jobs_dir> <max_threads> <max_backups> <register_fifo_name>
```

| Argument | Description |
|---|---|
| `jobs_dir` | Directory containing `.job` files to process |
| `max_threads` | Maximum number of worker threads |
| `max_backups` | Maximum number of concurrent backups |
| `register_fifo_name` | Name for the register FIFO (e.g. `server1`) |

**Example:**

```bash
./src/server/kvs ./jobs 4 2 server1
```

The server creates the FIFO `/tmp/server1` and starts processing all `.job` files in the given directory.

## Running the Client

```bash
./src/client/client <client_id> <register_fifo_name>
```

| Argument | Description |
|---|---|
| `client_id` | Unique client identifier (used to name its FIFOs) |
| `register_fifo_name` | Register FIFO name of the server (must match) |

**Example:**

```bash
./src/client/client client1 server1
```

## Server Commands (`.job` files)

Job files contain a sequence of commands, one per line:

```
WRITE [(key,value)(key2,value2)]
READ [key,key2]
DELETE [key]
SHOW
WAIT <ms>
BACKUP
```

For each `foo.job` file, a `foo.out` file is generated with the output.

**Example** (`jobs/example.job`):
```
WRITE [(name,Alice)(age,30)]
READ [name]
SHOW
DELETE [age]
```

## Client Commands (stdin)

```
SUBSCRIBE [key]       # subscribe to notifications for a key
UNSUBSCRIBE [key]     # cancel a subscription
DELAY <ms>            # wait N milliseconds
DISCONNECT            # disconnect from the server
```

When a subscribed key is updated or deleted on the server, the client automatically receives a notification:
```
(name,Bob)        # key updated
(age,DELETED)     # key deleted
```

## Sending SIGUSR1 to the Server

To disconnect all connected clients without stopping the server:

```bash
kill -SIGUSR1 <server_pid>
```

## Project Structure

```
src/
├── common/       # shared code (protocol, I/O, constants)
├── server/       # KVS server and job processing
├── client/       # interactive client
└── tests/        # client test files
```