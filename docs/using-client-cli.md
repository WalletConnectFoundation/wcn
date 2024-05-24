## Installing tools & dependencies

### Installing Rust

From `https://www.rust-lang.org/tools/install`

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Installing Docker

[follow these instructions](https://docs.docker.com/engine/install/)

### Installing Just (optional)

[Follow these instructions](https://github.com/casey/just?tab=readme-ov-file#installation)

## Configuting and running nodes locally

### Running nodes with Docker Compose

```bash
docker compose -f ./docker-compose.yml up -d
```

### Running nodes with Docker Compose (and Just)

```bash
just run-docker
```

## Using the CLI

### Build a release binary of the CLI

```bash
cargo build --release -p irn
```

### Running the CLI binary

The compiled binary is normally located at `target/release/irn`

```bash
chmod +x ./target/release/irn

./target/release/irn
```

You should see output like this:

```bash
Control nodes and clusters in the IRN Network

Usage: irn <COMMAND>

Commands:
  node     Node control subcommands
  key      Manage node and client keys
  storage  Execute storage commands on a cluster
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

Alternatively, to compile and run a debug build of the CLI binary you can do:

```bash
cargo run -p irn
```

### Generating a keypair

```bash
irn key generate
```

You should see an output as follows

```bash
Key 0
Private key: DFu899OZ7vsRJ0wDmApHqEKuklvN3KnigGLqguJF+54=
Public key: F4sT/gmL87wcrPXmkdOCZQVOijiRGJzAeLM6NCnSu8U=
Peer ID: 12D3KooWBQGYh92KEdxUW5UUdy7BvAW96hvYLBMmjzy6X2XsY2HA
```

### Running a node using the CLI

```bash
irn node start -w ./working-dir -c ./config.toml
```

**Note:** Interrupting a running node via `ctrl+c`/`SIGINT` would put the node into the `restarting` state, which assumes a very short downtime. Having one node in the cluster in `restarting` state prevents other nodes from restarting or leaving the cluster. If the node is being shutdown for a long period of time, it should be decommissioned instead. Decommissioning can only be done externally (see below).

### Running a node in detached mode

```bash
irn node start -w ./working-dir -c ./config.toml -d
```

### Stopping a node for restart

```bash
irn node stop -w ./working-dir -r
```

### Stopping and decomissioning node

```bash
irn node stop -w ./working-dir
```

### Interacting with a running node

```bash
irn storage
```

### Writing values to a node

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    set "user123" "exampleData"
```

### Reading values

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    get "user123"
```

### Deleting values

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    del "user123"
```

### Setting map values

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hset "userDetails" "email" "user@example.com"
```

### Reading map values

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hget "userDetails" "email"
```

### Deleting a value from a map

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hdel "userDetails" "email"
```

### Listing all fields of map

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hfields "userDetails"
```

### Listing all values of a map

```bash
irn storage \
    --address ":3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hvals "userDetails"
```
