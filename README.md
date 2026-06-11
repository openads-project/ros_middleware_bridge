# middleware_bridge

> Vibe-coded!

Generic ROS 2 middleware bridge for two endpoint processes running different `RMW_IMPLEMENTATION` values.
The default launch setup bridges ROS 2 data between Fast DDS (`rmw_fastrtps_cpp`) and Zenoh (`rmw_zenoh_cpp`) setup. It forwards serialized ROS messages between the two bridge endpoints, so other RMW pairings such as Fast DDS and Cyclone DDS (`rmw_cyclonedds_cpp`) can be used as well.

If both endpoints are DDS-based, direct DDS interoperability may already be sufficient. This bridge is mainly useful when the two ROS graphs should be isolated, need explicit topic-level forwarding, or need the bridge's UDP/SHM transport behavior.

The bridge forwards `rclcpp::SerializedMessage` data:
- Side A subscribes on configured local topics and forwards serialized bytes via the configured transport (`udp` or `shm`).
- Side B receives those bytes and republishes them as the original ROS message type.
- The same applies in the opposite direction.

## Architecture

Both processes run the same node (`middleware_bridge`) and use one shared configuration file:
- `config/params.yml`

Flows are defined once per direction:
- `side_a2b.topics`, `side_a2b.topic_types`, `side_a2b.transports`, `side_a2b.qos_depths`
- `side_b2a.topics`, `side_b2a.topic_types`, `side_b2a.transports`, `side_b2a.qos_depths`
- Optional auto-discovery mode: set `<direction>.topics` to `["__auto__"]` (or leave it unset) and list only `<direction>.topic_types`.

Each process sets its bridge side through `bridge_side`:
- `bridge_side: a` selects side A.
- `bridge_side: b` selects side B.


The node derives `tx-only` / `rx-only` channels automatically.

Transport is configurable per topic (or per type in auto-discovery mode):
- `udp` for smaller messages and remote-capable communication
- `shm` (shared memory) for large local messages (for example `Image`, `PointCloud2`)

## Start

```bash
ros2 launch middleware_bridge middleware_bridge_launch.py
```

Default:
- Node `bridge_fast` with `RMW_IMPLEMENTATION=rmw_fastrtps_cpp`
- Node `bridge_zenoh` with `RMW_IMPLEMENTATION=rmw_zenoh_cpp`

For example, Fast DDS -> Cyclone DDS only changes the launch-side RMW mapping:

```bash
ros2 launch middleware_bridge middleware_bridge_launch.py \
  side_a_rmw_implementation:=rmw_fastrtps_cpp \
  side_b_rmw_implementation:=rmw_cyclonedds_cpp
```

Useful launch arguments:
- `side_a_name`, `side_b_name`
- `side_a_rmw_implementation`, `side_b_rmw_implementation`
- `side_a_tx_port`, `side_a_rx_port`, `side_b_tx_port`, `side_b_rx_port`

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `num_threads` | `int` | Number of threads for `MultiThreadedExecutor` |
| `bridge_side` | `string` | Bridge side selector: `a` or `b` |
| `remote_host` | `string` | IPv4 destination for UDP send |
| `shm_namespace` | `string` | Namespace prefix for shared-memory channel names |
| `tx_port` | `int` | UDP transmit port |
| `rx_port` | `int` | UDP receive port |
| `socket_buffer_bytes` | `int` | Socket send/receive buffer size |
| `max_udp_payload_bytes` | `int` | Maximum UDP datagram size per fragment (including header) |
| `max_shm_message_bytes` | `int` | Maximum message size per shared-memory channel |
| `shm_poll_interval_us` | `int` | Poll interval for shared-memory receiver loop |
| `reassembly_timeout_ms` | `int` | Timeout for incomplete UDP fragment reassembly |
| `auto_discovery_wait_ms` | `int` | Optional wait before the initial auto-discovery scan (0 disables waiting) |
| `auto_discovery_poll_ms` | `int` | Poll interval for runtime auto-discovery scans |
| `side_a2b.topics` | `string[]` | Side A -> side B. Static mode: explicit topics. Auto mode: set `["__auto__"]` (or omit value) and use `side_a2b.topic_types` only |
| `side_a2b.topic_types` | `string[]` | Side A -> side B. Static mode: type per topic. Auto mode: type filters for discovered topics |
| `side_a2b.transports` | `string[]` | Side A -> side B. `udp` or `shm` (empty/single/per-topic in static mode, empty/single/per-type in auto mode) |
| `side_a2b.qos_depths` | `int[]` | Side A -> side B. Minimum/fallback QoS KeepLast depth (empty/single/per-topic in static mode, empty/single/per-type in auto mode) |
| `side_b2a.topics` | `string[]` | Side B -> side A. Static mode: explicit topics. Auto mode: set `["__auto__"]` (or omit value) and use `side_b2a.topic_types` only |
| `side_b2a.topic_types` | `string[]` | Side B -> side A. Static mode: type per topic. Auto mode: type filters for discovered topics |
| `side_b2a.transports` | `string[]` | Side B -> side A. `udp` or `shm` (empty/single/per-topic in static mode, empty/single/per-type in auto mode) |
| `side_b2a.qos_depths` | `int[]` | Side B -> side A. Minimum/fallback QoS KeepLast depth (empty/single/per-topic in static mode, empty/single/per-type in auto mode) |

## Notes

- Topic order matters: both nodes must use identical channel ordering from the shared config.
- Auto-discovery runs at startup and then continuously with `auto_discovery_poll_ms`.
- Auto-discovery scans only on the source side of each direction (`side_a2b` on `bridge_side=a`, `side_b2a` on `bridge_side=b`).
- The destination side creates matching channels from UDP control announcements.
- Newly appearing matching topics are added at runtime with the configured per-type `transport` and source-side QoS.
- Reliability, durability, history, and depth are read from source publisher endpoints and announced to the remote bridge.
- `qos_depths` remains a minimum/fallback depth for normal topics.
- Output publishers on `/tf_static` always use reliable, keep-last depth 1, transient-local QoS.
- Forwarded `/tf_static` messages are aggregated by `child_frame_id` before publishing, so late joiners receive the complete static transform set in the latched sample.
- Auto-discovered and static source channels are announced to the remote bridge over UDP control packets.
- The control path therefore requires valid UDP connectivity (`remote_host`, `tx_port`, `rx_port`) even when data transport is `shm`.
- For `shm`, ensure `/dev/shm` is large enough for your configured channel capacity (`max_shm_message_bytes`) and number of SHM channels.
- The default `params.yml` contains CARLA-specific flows using the generic direction names (`side_a2b` and `side_b2a`) used in this project.
- Large messages (for example `Image`, `PointCloud2`) can run on `shm` per topic, or on fragmented `udp`.
- `shm` is local-host only; use `udp` for distributed setups.
- SHM channels currently use latest-sample behavior (older samples can be overwritten under load).
- UDP support is currently IPv4-only.
