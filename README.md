# irn-node

Inter-Relay-Network Node. Implemented in Rust.

## Running Locally

Setup:

- Install [`rust`](https://www.rust-lang.org/tools/install);
- Install [`docker`](https://docs.docker.com/get-docker/);
- Install [`just`](https://github.com/casey/just#packages);
- Copy the env file:
  ```sh
  $ cp .env.example .env
  ```
- Fill `.env` file with necessary values - access keys for AWS and other services. No changes are necessary if just running locally.

Running the IRN Node:

```sh
just run
```

Running checks & tests:

```sh
just devloop
```

Running all checks & tests including storage tests:

```sh
just devloop test-all
```

## Running a sandbox cluster

Running dockerized sandbox cluster:

```sh
$ just run-docker
```

## Contributing

If you would like to contribute to the project, please read
the [Contributing](./docs/Contributing.md) guide.

## License

Licensed under [Apache License, Version 2.0](./LICENSE).

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be licensed as above, without any additional terms or conditions.
