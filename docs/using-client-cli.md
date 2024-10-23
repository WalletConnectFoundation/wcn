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
You should see output like this:

```bash
Key 0
Private key: DFu899OZ7vsRJ0wDmApHqEKuklvN3KnigGLqguJF+54=
Public key: F4sT/gmL87wcrPXmkdOCZQVOijiRGJzAeLM6NCnSu8U=
Peer ID: 12D3KooWBQGYh92KEdxUW5UUdy7BvAW96hvYLBMmjzy6X2XsY2HA
```

### Interacting with a running node

```bash
irn storage
```

### Writing values to a node

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    set "user123" "exampleData"
```

### Reading values

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    get "user123"
```

### Deleting values

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    del "user123"
```

### Setting map values

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hset "userDetails" "email" "user@example.com"
```

### Reading map values

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hget "userDetails" "email"
```

### Deleting a value from a map

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hdel "userDetails" "email"
```

### Listing all fields of map

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hfields "userDetails"
```

### Listing all values of a map

```bash
irn storage \
    --address "127.0.0.1:3011" \
    --private-key "<YOUR_PRIVATE_KEY>" \
    hvals "userDetails"
```

### Running a node using the CLI
Please be advised, running a node via the CLI is intended for development purposes only and subject to change in the future. In production environments, please run either the irn_node binary or one of the docker images our team provides and configure them via environment variables instead.

```bash
irn node start -w ./working-dir -c ./config.toml
```

Minimal config file for a **non-bootstrap** node would look like the following:

```toml
# Node identity.
[identity]
# ed25519 private key encoded as base64. Can be generated using `irn key generate` command.
private_key = "yDt4KicJ5aJF8nS4zYtDPrel2WJcU6cD7hO+u5m5Arg="
# Replication group this node will belong to. Should correlate with availability zones.
group = 1
# ETH address of the node operator.
eth_address = "0xeD380b8da6012c20856CEc076C348AC44fA50f63"

# List of known nodes. Should contain at least one node to be able to join the cluster.
[[known_peers]]
id = "12D3KooWHFJ8gjwjw4e6irVouqpiPs9EpqN4nD4GrJ8ZfobheDeH"
group = 1
address = "127.0.0.1:3010"

# The node's address on the network. IP address should be accessible to other nodes.
[server]
bind_address = "127.0.0.1"
server_port = 3050
client_port = 3051
metrics_port = 3052

[authorization]
# Whitelist of client peer IDs this node would allow to connect.
clients = ["12D3KooWSjVfmYoyEQD4GFR2YXmcRvzZJx1SuNuaLfhmZydSpUzm"]
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
