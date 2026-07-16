# pubky-watcher examples

## `poll_homeserver`

Minimal end-to-end example of the generic watcher pipeline:

1. Initialise [`PubkyConnector`](../src/client/connector.rs) (testnet or mainnet)
2. Build a [`TEventProcessorRunner`](../src/runner.rs) for one homeserver public key
3. Poll `GET https://{homeserver}/events/?cursor=…&limit=…`
4. Parse lines and print them via a simple [`EventHandler`](../src/pipeline.rs)

### Prerequisites (testnet)

The default homeserver key is the static testnet HS:

```text
8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo
```

Start a local testnet first with [pubky-antfarm](https://github.com/tipogi/pubky-antfarm) (isolated DHT, homeservers, and optional simulated social activity):

```bash
cargo run -p pubky-testnet
```

See the antfarm README for dashboard setup and seeding commands.

### Run

From the workspace root:

```bash
# Default: testnet client + static testnet homeserver
cargo run -p pubky-watcher --example poll_homeserver

# Custom homeserver key / cursor / batch size
cargo run -p pubky-watcher --example poll_homeserver -- \
  --homeserver 8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo \
  --cursor 0 \
  --limit 50

# Poll a few times
cargo run -p pubky-watcher --example poll_homeserver -- --ticks 5 --interval-ms 2000

# Mainnet homeserver (disable testnet client)
cargo run -p pubky-watcher --example poll_homeserver -- \
  --homeserver <z32-pubkey> \
  --no-testnet
```

Logging uses `RUST_LOG` (default `info`), for example:

```bash
RUST_LOG=debug cargo run -p pubky-watcher --example poll_homeserver
```
