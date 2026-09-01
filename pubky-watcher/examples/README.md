# pubky-watcher examples

## `poll_homeserver`

Demonstrates [`TEventProcessor`](../src/processor.rs) and
[`TEventProcessorRunner`](../src/runner.rs) by polling a public staging
homeserver five times. Each tick requests up to eight events from the latest
cursor and parses the response with [`EventBatch`](../src/events.rs).

Events are printed through an [`EventHandler`](../src/traits.rs). For `PUT`
events, the referenced resource is fetched and its body is printed when it is
text or JSON.

### Staging homeserver

The example connects directly to the following public staging homeserver:

```text
ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy
```

### Run

```bash
cargo run -p pubky-watcher --example poll_homeserver
```
