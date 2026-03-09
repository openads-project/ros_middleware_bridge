# middleware_bridge

> Vibe-coded!

Generic ROS 2 middleware bridge for two processes running different `RMW_IMPLEMENTATION` values (for example `rmw_fastrtps_cpp` and `rmw_zenoh_cpp`).

The bridge forwards `rclcpp::SerializedMessage` data:
- Process A subscribes on configured local topics and forwards serialized bytes via the configured transport (`udp` or `shm`).
- Process B receives those bytes and republishes them as the original ROS message type.
- The same applies in the opposite direction.

## Architecture

Both processes run the same node (`middleware_bridge`) and use one shared configuration file:
- `config/params.yml`

Flows are defined once per direction:
- `dds2zenoh.topics`, `dds2zenoh.topic_types`, `dds2zenoh.transports`, `dds2zenoh.qos_depths`
- `zenoh2dds.topics`, `zenoh2dds.topic_types`, `zenoh2dds.transports`, `zenoh2dds.qos_depths`

Each process only sets its role:
- `bridge_role: dds` for `bridge_fast`
- `bridge_role: zenoh` for `bridge_zenoh`

The node derives `tx-only` / `rx-only` channels automatically.

Transport is configurable per topic:
- `udp` for smaller messages and remote-capable communication
- `shm` (shared memory) for large local messages (for example `Image`, `PointCloud2`)

## Start

```bash
ros2 launch middleware_bridge middleware_bridge_launch.py
```

Default:
- Node `bridge_fast` with `RMW_IMPLEMENTATION=rmw_fastrtps_cpp`
- Node `bridge_zenoh` with `RMW_IMPLEMENTATION=rmw_zenoh_cpp`

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `num_threads` | `int` | Number of threads for `MultiThreadedExecutor` |
| `bridge_role` | `string` | `dds` or `zenoh` |
| `remote_host` | `string` | IPv4 destination for UDP send |
| `shm_namespace` | `string` | Namespace prefix for shared-memory channel names |
| `tx_port` | `int` | UDP transmit port |
| `rx_port` | `int` | UDP receive port |
| `socket_buffer_bytes` | `int` | Socket send/receive buffer size |
| `max_udp_payload_bytes` | `int` | Maximum UDP datagram size per fragment (including header) |
| `max_shm_message_bytes` | `int` | Maximum message size per shared-memory channel |
| `shm_poll_interval_us` | `int` | Poll interval for shared-memory receiver loop |
| `reassembly_timeout_ms` | `int` | Timeout for incomplete UDP fragment reassembly |
| `dds2zenoh.topics` | `string[]` | Topics bridged from DDS to Zenoh |
| `dds2zenoh.topic_types` | `string[]` | Types for `dds2zenoh.topics` |
| `dds2zenoh.transports` | `string[]` | `udp` or `shm` (empty, single global value, or per-topic) |
| `dds2zenoh.qos_depths` | `int[]` | QoS KeepLast depth (empty, single global value, or per-topic) |
| `zenoh2dds.topics` | `string[]` | Topics bridged from Zenoh to DDS |
| `zenoh2dds.topic_types` | `string[]` | Types for `zenoh2dds.topics` |
| `zenoh2dds.transports` | `string[]` | `udp` or `shm` (empty, single global value, or per-topic) |
| `zenoh2dds.qos_depths` | `int[]` | QoS KeepLast depth (empty, single global value, or per-topic) |

## Notes

- Topic order matters: both nodes must use identical channel ordering from the shared config.
- The default `params.yml` contains CARLA-specific flows (`dds2zenoh` and `zenoh2dds`) used in this project.
- Large messages (for example `Image`, `PointCloud2`) can run on `shm` per topic, or on fragmented `udp`.
- `shm` is local-host only; use `udp` for distributed setups.
- SHM channels currently use latest-sample behavior (older samples can be overwritten under load).
- UDP support is currently IPv4-only.
