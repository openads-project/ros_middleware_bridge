# middleware_bridge

Generische ROS 2 Middleware-Bridge für zwei Prozesse mit unterschiedlichen `RMW_IMPLEMENTATION`-Werten (z. B. `rmw_fastrtps_cpp` und `rmw_zenoh_cpp`).

Die Bridge arbeitet vollständig mit `rclcpp::SerializedMessage`:
- Prozess A subscribed lokal auf konfigurierten Topics, serialisiert und sendet über den konfigurierten Transport (`udp` oder `shm`).
- Prozess B empfängt die Bytes und published sie wieder als Original-Message-Typ.
- Symmetrisch in die Gegenrichtung.

## Architektur

Beide Prozesse nutzen denselben Node (`middleware_bridge`) und dieselbe zentrale Konfiguration:
- `config/params.yml`

Die Flows werden einmalig als Richtungen definiert:
- `dds2zenoh.topics`, `dds2zenoh.topic_types`, `dds2zenoh.transports`, `dds2zenoh.qos_depths`
- `zenoh2dds.topics`, `zenoh2dds.topic_types`, `zenoh2dds.transports`, `zenoh2dds.qos_depths`

Jeder Node setzt nur noch seine Rolle:
- `bridge_role: dds` für `bridge_fast`
- `bridge_role: zenoh` für `bridge_zenoh`

Der Node baut daraus automatisch `tx-only`/`rx-only`-Kanäle.

Der Transport ist pro Topic konfigurierbar:
- `udp` für kleine Nachrichten / Remote-fähig
- `shm` (Shared Memory) für große lokale Nachrichten (z. B. `Image`, `PointCloud2`)

## Starten

```bash
ros2 launch middleware_bridge middleware_bridge_launch.py
```

Default:
- Node `bridge_fast` mit `RMW_IMPLEMENTATION=rmw_fastrtps_cpp`
- Node `bridge_zenoh` mit `RMW_IMPLEMENTATION=rmw_zenoh_cpp`

## Parameter

| Parameter | Typ | Beschreibung |
| --- | --- | --- |
| `num_threads` | `int` | Threads für `MultiThreadedExecutor` |
| `bridge_role` | `string` | `dds` oder `zenoh` |
| `remote_host` | `string` | IPv4-Zieladresse für UDP-Send |
| `shm_namespace` | `string` | Namespace für SHM-Kanalnamen |
| `tx_port` | `int` | UDP-Sendeport zur Gegenstelle |
| `rx_port` | `int` | UDP-Port, auf dem empfangen wird |
| `socket_buffer_bytes` | `int` | Send/Receive Socket Buffer |
| `max_udp_payload_bytes` | `int` | Maximale UDP-Datagrammgröße pro Fragment (inkl. Header) |
| `max_shm_message_bytes` | `int` | Maximale SHM-Nachrichtengröße pro Kanal |
| `shm_poll_interval_us` | `int` | Polling-Intervall für SHM-Empfang |
| `reassembly_timeout_ms` | `int` | Timeout für unvollständige Fragment-Reassemblies |
| `dds2zenoh.topics` | `string[]` | Topics in Richtung DDS -> Zenoh |
| `dds2zenoh.topic_types` | `string[]` | Typen zu `dds2zenoh.topics` |
| `dds2zenoh.transports` | `string[]` | `udp` oder `shm` (leer, 1 global oder pro Topic) |
| `dds2zenoh.qos_depths` | `int[]` | QoS (leer, 1 global oder pro Topic) |
| `zenoh2dds.topics` | `string[]` | Topics in Richtung Zenoh -> DDS |
| `zenoh2dds.topic_types` | `string[]` | Typen zu `zenoh2dds.topics` |
| `zenoh2dds.transports` | `string[]` | `udp` oder `shm` (leer, 1 global oder pro Topic) |
| `zenoh2dds.qos_depths` | `int[]` | QoS (leer, 1 global oder pro Topic) |

## Hinweise

- Topic-Reihenfolge ist relevant: beide Nodes verwenden dieselbe Kanalreihenfolge aus der gemeinsamen Datei.
- Die Default-`params.yml` enthält die konkreten CARLA-Flows (`dds2zenoh` und `zenoh2dds`) aus diesem Projekt.
- Große Messages (z. B. `Image`, `PointCloud2`) können pro Topic über `shm` laufen oder über fragmentiertes `udp`.
- `shm` funktioniert nur auf derselben Maschine; für verteilte Setups `udp` verwenden.
- SHM-Kanäle nutzen aktuell Latest-Sample-Verhalten (bei Überlast wird die ältere Probe überschrieben).
- Aktuell ist UDP auf IPv4 ausgelegt.
