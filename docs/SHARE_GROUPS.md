# Share groups (KIP-932 — Queues for Kafka)

Share groups give Kafka **queue semantics**: many consumers can read from the same
partition cooperatively, each record is delivered to one consumer at a time under
a per-record **acquisition lock**, and records are individually acknowledged
(Accept / Release / Reject) rather than committed by offset. GoKafka implements
the full **client** side (the broker side is Kafka 4.1+ with `share.version`
enabled; GA in Kafka 4.2).

> Requires a broker with share groups enabled. On a broker without it, `Poll`
> returns a clear *"broker does not support KIP-932 share groups …"* error, and
> the admin/offset calls return a not-supported error.

## Consuming

```go
cfg, _ := gokafka.NewConfig(brokers,
    gokafka.WithShareGroup("orders-workers"),
    gokafka.WithShareAutoOffsetReset("earliest"), // earliest | latest | by_duration:PT1H
    gokafka.WithShareAcknowledgementMode(gokafka.ShareAckImplicit), // or the default explicit
)
client, _ := gokafka.NewClient(cfg)
share := client.ShareConsumer([]string{"orders"})
defer share.Leave(context.Background())

for {
    records, err := share.Poll(ctx) // acquire a batch (per-record lock held)
    if err != nil { /* ... */ }
    for _, r := range records {
        // r.DeliveryCount: 1 on first delivery, +1 on each redelivery.
        if err := process(r); err != nil {
            _ = share.Release(ctx, r) // return to the group for redelivery
            continue
        }
    }
    _ = share.Acknowledge(ctx, records...) // Accept: settled, not redelivered
}
```

### Acknowledgement

| Call | Meaning |
|---|---|
| `Acknowledge` | **Accept** — settled; not redelivered. |
| `Release` | Return to the group; redelivered to some consumer (increments `DeliveryCount`). |
| `Reject` | Poison message; archived, never redelivered. |
| `Renew` | Extend the acquisition lock for long processing (needs ShareAcknowledge v2 / Kafka 4.3+). |

- **Explicit mode** (default): you call Accept/Release/Reject for each batch.
- **Implicit mode** (`WithShareAcknowledgementMode(ShareAckImplicit)`): the batch a
  `Poll` returns is auto-Accepted on the next `Poll` (or on `Leave`), matching the
  Java `KafkaShareConsumer` default — a plain consume loop settles records with no
  per-batch bookkeeping.

### Dead-letter pattern with `DeliveryCount`

`Record.DeliveryCount` is the KIP-932 delivery attempt count. Route a record to a
DLQ (or `Reject` it) once it approaches the group's delivery-count limit (default 5):

```go
if r.DeliveryCount >= 5 {
    _ = produceToDLQ(r)
    _ = share.Reject(ctx, r)
} else if err := process(r); err != nil {
    _ = share.Release(ctx, r)
}
```

## Admin

```go
admin := client.Admin()

// Where has the group consumed to? (share-partition start offset)
offs, _ := admin.DescribeShareGroupOffsets(ctx, "orders-workers")

// How far behind is it? (log-end offset − SPSO, per partition)
lag, _ := admin.ShareGroupLag(ctx, "orders-workers")
for _, l := range lag {
    fmt.Printf("%s[%d] lag=%d\n", l.Topic, l.Partition, l.Lag)
}

// Reset offsets to reprocess (group must be empty), or delete them.
_ = admin.AlterShareGroupOffsets(ctx, "orders-workers", map[string]map[int32]int64{"orders": {0: 0}})
_ = admin.DeleteShareGroupOffsets(ctx, "orders-workers", "orders")

// Group-level configs (GROUP config resource, type 32).
_ = admin.AlterGroupConfigs(ctx, "orders-workers", map[string]*string{
    "share.isolation.level":   ptr("read_committed"),
    "share.auto.offset.reset": ptr("latest"),
})
cfgs, _ := admin.DescribeGroupConfigs(ctx, "orders-workers")

// Describe members / state.
descs, _ := admin.DescribeShareGroups(ctx, "orders-workers")
```

## Configuration reference

Consumer options: `WithShareGroup`, `WithShareAcknowledgementMode`,
`WithShareAutoOffsetReset`, `WithConsumeFromBeginning` (forces `earliest`),
`WithIsolationLevel(ReadCommitted)` → applied as `share.isolation.level`.

Group configs (via `AlterGroupConfigs`): `share.isolation.level`,
`share.auto.offset.reset` (`earliest`/`latest`/`by_duration:…`),
`share.record.lock.duration.ms`, `share.session.timeout.ms`,
`share.heartbeat.interval.ms`, `share.delivery.count.limit`.

## Wire APIs

Client RPCs: ShareGroupHeartbeat (76), ShareGroupDescribe (77), ShareFetch (78),
ShareAcknowledge (79, v2 adds Renew). Admin RPCs: DescribeShareGroupOffsets (90),
AlterShareGroupOffsets (91), DeleteShareGroupOffsets (92).

## Compatibility

Kafka 4.1+ with `share.version ≥ 1` (GA in 4.2). **Redpanda does not support share
groups** as of v26.1.x (GoKafka's suite auto-skips them). See
[`REDPANDA.md`](REDPANDA.md) and [`CONFORMANCE.md`](CONFORMANCE.md).

## Example

Runnable: [`examples/sharegroup`](../examples/sharegroup) (consume) and
[`examples/sharegroupadmin`](../examples/sharegroupadmin) (offsets, lag, configs,
delivery-count DLQ).
