# `ros_middleware_bridge`

ROS 2 middleware bridge serializing topic data between different RMW implementations

## Nodes

### `ros_middleware_bridge`

Generic ROS 2 middleware bridge for two endpoint processes that may run with
different `RMW_IMPLEMENTATION` values. The default launch setup starts one
endpoint with Fast DDS (`rmw_fastrtps_cpp`) and one endpoint with Zenoh
(`rmw_zenoh_cpp`), but other pairings such as Fast DDS and Cyclone DDS
(`rmw_cyclonedds_cpp`) can be used as well.

The bridge forwards `rclcpp::SerializedMessage` data between both sides:
- Side A subscribes on configured local topics and sends serialized bytes via
  the configured transport (`udp` or `shm`).
- Side B receives those bytes and republishes them as the original ROS message
  type.
- The same forwarding model is used in the opposite direction.

Both endpoint processes run the same node executable and use the shared
`config/params.yml` configuration file. Flows are configured per direction with
`side_a2b.*` and `side_b2a.*` parameters. Each process selects its role through
`bridge_side` (`a` or `b`), while the node derives matching transmit-only and
receive-only channels automatically.

Use this bridge when two ROS graphs should be isolated, need explicit
topic-level forwarding, or should use the bridge's UDP/shared-memory transport
behavior. If both endpoints are DDS-based, direct DDS interoperability may
already be sufficient.

> [!NOTE]
> - Topic order matters: both nodes must use identical channel ordering from the
  shared config.
> - Auto-discovery runs at startup and then continuously with
  `auto_discovery_poll_ms`.
> - Auto-discovery scans only on the source side of each direction (`side_a2b` on
  `bridge_side=a`, `side_b2a` on `bridge_side=b`).
> - The destination side creates matching channels from UDP control announcements.
> - Newly appearing matching topics are added at runtime with the configured
  per-type transport and source-side QoS.
> - Reliability, durability, history, and depth are read from source publisher
  endpoints and announced to the remote bridge.
> - `qos_depths` remains a minimum/fallback depth for normal topics.
> - Output publishers on `/tf` always use volatile durability.
> - Output publishers on `/tf_static` always use reliable, keep-last depth 1,
  transient-local QoS.
> - Forwarded `/tf_static` messages are aggregated by `child_frame_id` before
  publishing, so late joiners receive the complete static transform set in the
  latched sample.
> - Auto-discovered and static source channels are announced to the remote bridge
  over UDP control packets.
> - The control path therefore requires valid UDP connectivity (`remote_host`,
  `tx_port`, `rx_port`) even when data transport is `shm`.
> - For `shm`, ensure `/dev/shm` is large enough for your configured channel
  capacity (`max_shm_message_bytes`) and number of SHM channels.
> - The default `params.yml` contains generic example flows using the direction
  names `side_a2b` and `side_b2a`.
> - Large messages, for example `Image` or `PointCloud2`, can run on `shm` per
  topic or on fragmented `udp`.
> - `shm` is local-host only; use `udp` for distributed setups.
> - SHM channels currently use latest-sample behavior, so older samples can be
  overwritten under load.
> - UDP support is currently IPv4-only.

#### Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `num_threads` | `int` | `1` | Number of threads used by the rclcpp MultiThreadedExecutor |
| `bridge_side` | `string` | `"a"` | Bridge side selector: a or b |
| `remote_host` | `string` | `"127.0.0.1"` | IPv4 destination used for UDP sends |
| `shm_namespace` | `string` | `"ros_middleware_bridge"` | Namespace prefix used for shared-memory channel names |
| `tx_port` | `int` | `17001` | UDP transmit port |
| `rx_port` | `int` | `17002` | UDP receive port |
| `socket_buffer_bytes` | `int` | `1024 * 1024` | UDP socket send/receive buffer size in bytes |
| `max_udp_payload_bytes` | `int` | `60000` | Maximum UDP datagram payload per fragment, including the bridge fragment header |
| `max_shm_message_bytes` | `int` | `8 * 1024 * 1024` | Maximum message size per shared-memory channel in bytes |
| `shm_poll_interval_us` | `int` | `1000` | Poll interval for the shared-memory receiver loop in microseconds |
| `reassembly_timeout_ms` | `int` | `1000` | Timeout for incomplete UDP fragment reassembly in milliseconds |
| `auto_discovery_wait_ms` | `int` | `0` | Optional wait before the initial auto-discovery scan in milliseconds; 0 disables the wait |
| `auto_discovery_poll_ms` | `int` | `1000` | Poll interval for runtime auto-discovery scans in milliseconds |
| `side_a2b.qos_depths` | `int[]` | `[]` | Side A to side B minimum/fallback QoS KeepLast depth per topic/type |
| `side_b2a.qos_depths` | `int[]` | `[]` | Side B to side A minimum/fallback QoS KeepLast depth per topic/type |

## Launch Files

### [`ros_middleware_bridge.launch.py`](launch/ros_middleware_bridge.launch.py)

| Argument | Default | Description |
| --- | --- | --- |
| `params` | `os.path.join(get_package_share_directory("ros_middleware_bridge"), "config", "params.yml")` | path to parameter file |
| `namespace` | `""` | node namespace |
| `log_level` | `"info"` | ROS logging level (debug, info, warn, error, fatal) |
| `use_sim_time` | `"false"` | use simulation clock |
| `side_a_name` | `"bridge_fast"` | side A node name (default Fast DDS side) |
| `side_a_rmw_implementation` | `"rmw_fastrtps_cpp"` | RMW_IMPLEMENTATION for side A (project default: Fast DDS) |
| `side_a_tx_port` | `"17001"` | UDP transmit port for side A |
| `side_a_rx_port` | `"17002"` | UDP receive port for side A |
| `side_b_name` | `"bridge_zenoh"` | side B node name (default Zenoh side) |
| `side_b_rmw_implementation` | `"rmw_zenoh_cpp"` | RMW_IMPLEMENTATION for side B (project default: Zenoh) |
| `side_b_tx_port` | `"17002"` | UDP transmit port for side B |
| `side_b_rx_port` | `"17001"` | UDP receive port for side B |
