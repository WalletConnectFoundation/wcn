## Instructions on how to showcase this example

Run the sandbox cluster:
```sh
just run-docker-irn
```

Build the example (within `/crate/irn_bin`)
```
cargo build --example sandbox-cluster-client --release
```

Open 5 terminals for each sandbox cluster node and start following docker logs
```sh
docker logs {container-id} -n 2 -f
```

Open another terminal and try out the following commands (within the root dir)
```sh
./target/release/examples/sandbox-cluster-client -n 1 set a 1

./target/release/examples/sandbox-cluster-client -n 2 get a

./target/release/examples/sandbox-cluster-client -n 3 set b 2

./target/release/examples/sandbox-cluster-client -n 3 get b
```

See that on each request 3-4 out of 5 nodes are having some `info` logs about handling the request.

Check out `--help` and try out different keys/values
```
Usage: sandbox-cluster-client --node-id <NODE_ID> <COMMAND>

Commands:
  get
  set
  help  Print this message or the help of the given subcommand(s)

Options:
  -n, --node-id <NODE_ID>  Specifies the "id" of the node to make a request to
  -h, --help               Print help (see more with '--help')
```
```
Usage: sandbox-cluster-client --node-id <NODE_ID> set <KEY> <VALUE>

Arguments:
  <KEY>    Hex string up to 32 chars
  <VALUE>  Arbitrary string
```
```
Usage: sandbox-cluster-client --node-id <NODE_ID> get <KEY>

Arguments:
  <KEY>  Hex string up to 32 chars
```
