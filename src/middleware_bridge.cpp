#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <middleware_bridge/middleware_bridge.hpp>

namespace middleware_bridge {

MiddlewareBridge::MiddlewareBridge() : Node("middleware_bridge") {
  try {
    declareAndLoadParameters();
    setupBridgeChannels();

    if (use_udp_transport_) {
      setupSockets();
    }

    if (auto_discovery_enabled_ && auto_discovery_wait_ms_ > 0) {
      RCLCPP_INFO(this->get_logger(), "Waiting %d ms before initial auto-discovery scan.", auto_discovery_wait_ms_);
      std::this_thread::sleep_for(std::chrono::milliseconds(auto_discovery_wait_ms_));
    }
    if (auto_discovery_enabled_) {
      runAutoDiscoveryScan();
    }

    if (use_shm_transport_) {
      setupSharedMemoryChannels();
    }

    receiver_running_.store(true);
    if (use_udp_transport_) {
      udp_receiver_thread_ = std::thread(&MiddlewareBridge::receiverLoop, this);
    }
    if (use_shm_transport_) {
      shm_receiver_thread_ = std::thread(&MiddlewareBridge::shmReceiverLoop, this);
    }

    if (auto_discovery_enabled_ && auto_discovery_poll_ms_ > 0) {
      auto_discovery_timer_ = this->create_wall_timer(
          std::chrono::milliseconds(auto_discovery_poll_ms_),
          [this]() { this->runAutoDiscoveryScan(); });
    }
  } catch (...) {
    stopBackgroundThreads();
    closeSharedMemoryChannels();
    throw;
  }
}

MiddlewareBridge::~MiddlewareBridge() {
  stopBackgroundThreads();
  closeSharedMemoryChannels();
}

void MiddlewareBridge::declareAndLoadParameters() {
  auto declare_string_array_parameter = [this](const std::string & name,
                                               const std::vector<std::string> & default_value,
                                               bool fallback_to_empty_on_unset) -> std::vector<std::string> {
    try {
      return this->declare_parameter<std::vector<std::string>>(name, default_value);
    } catch (const rclcpp::exceptions::InvalidParameterValueException & ex) {
      const std::string message = ex.what();
      if (fallback_to_empty_on_unset && message.find("No parameter value set") != std::string::npos) {
        RCLCPP_WARN(
            this->get_logger(),
            "Parameter '%s' is set without value. Falling back to empty list.",
            name.c_str());
        return {};
      }
      throw;
    }
  };

  num_threads_ = this->declare_parameter<int>("num_threads", 1);
  bridge_role_ = this->declare_parameter<std::string>("bridge_role", "dds");
  remote_host_ = this->declare_parameter<std::string>("remote_host", "127.0.0.1");
  shm_namespace_ = this->declare_parameter<std::string>("shm_namespace", "middleware_bridge");
  tx_port_ = this->declare_parameter<int>("tx_port", 17001);
  rx_port_ = this->declare_parameter<int>("rx_port", 17002);
  socket_buffer_bytes_ = this->declare_parameter<int>("socket_buffer_bytes", 1024 * 1024);
  max_udp_payload_bytes_ = this->declare_parameter<int>("max_udp_payload_bytes", 60000);
  max_shm_message_bytes_ = this->declare_parameter<int>("max_shm_message_bytes", 8 * 1024 * 1024);
  shm_poll_interval_us_ = this->declare_parameter<int>("shm_poll_interval_us", 1000);
  reassembly_timeout_ms_ = this->declare_parameter<int>("reassembly_timeout_ms", 1000);
  auto_discovery_wait_ms_ = this->declare_parameter<int>("auto_discovery_wait_ms", 0);
  auto_discovery_poll_ms_ = this->declare_parameter<int>("auto_discovery_poll_ms", 1000);

  dds2zenoh_topics_ = declare_string_array_parameter(
      "dds2zenoh.topics", std::vector<std::string>{"/bridge/dds2zenoh/example"}, true);
  dds2zenoh_topic_types_ = declare_string_array_parameter(
      "dds2zenoh.topic_types", std::vector<std::string>{"geometry_msgs/msg/PointStamped"}, false);
  dds2zenoh_transports_ = declare_string_array_parameter("dds2zenoh.transports", std::vector<std::string>{}, false);
  dds2zenoh_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("dds2zenoh.qos_depths", std::vector<int64_t>{10});

  zenoh2dds_topics_ = declare_string_array_parameter("zenoh2dds.topics", std::vector<std::string>{}, true);
  zenoh2dds_topic_types_ = declare_string_array_parameter("zenoh2dds.topic_types", std::vector<std::string>{}, false);
  zenoh2dds_transports_ = declare_string_array_parameter("zenoh2dds.transports", std::vector<std::string>{}, false);
  zenoh2dds_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("zenoh2dds.qos_depths", std::vector<int64_t>{});

  auto normalize_auto_discovery_topics = [](std::vector<std::string> & topics) {
    if (topics.size() != 1U) {
      return;
    }
    std::string value = topics.front();
    std::transform(
        value.begin(),
        value.end(),
        value.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value.empty() || value == "__auto__" || value == "__auto_discovery__") {
      topics.clear();
    }
  };
  normalize_auto_discovery_topics(dds2zenoh_topics_);
  normalize_auto_discovery_topics(zenoh2dds_topics_);

  std::transform(
      bridge_role_.begin(),
      bridge_role_.end(),
      bridge_role_.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (bridge_role_ == "fast" || bridge_role_ == "fastrtps" || bridge_role_ == "fastrtps_cpp" ||
      bridge_role_ == "rmw_fastrtps_cpp" || bridge_role_ == "rmw_fast_rtps" || bridge_role_ == "rmw_fastdds_cpp") {
    bridge_role_ = "dds";
  }
  if (bridge_role_ == "zenoh_cpp" || bridge_role_ == "rmw_zenoh_cpp" || bridge_role_ == "rmw_zenoh_c" ||
      bridge_role_ == "zenoh") {
    bridge_role_ = "zenoh";
  }
  if (bridge_role_ != "dds" && bridge_role_ != "zenoh") {
    throw std::runtime_error(
        "Parameter 'bridge_role' must resolve to 'dds' or 'zenoh' (e.g. dds, fast, fastrtps_cpp, rmw_fastrtps_cpp, "
        "zenoh, zenoh_cpp, rmw_zenoh_cpp)."
    );
  }

  if (auto_discovery_wait_ms_ < 0) {
    throw std::runtime_error("Parameter 'auto_discovery_wait_ms' must be greater than or equal to zero.");
  }
  if (auto_discovery_poll_ms_ <= 0) {
    throw std::runtime_error("Parameter 'auto_discovery_poll_ms' must be greater than zero.");
  }

  auto canonical_transport = [](std::string transport) -> std::string {
    std::transform(
        transport.begin(),
        transport.end(),
        transport.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (transport == "udp") {
      return "udp";
    }
    if (transport == "shm" || transport == "shared_memory" || transport == "shared-memory") {
      return "shm";
    }
    throw std::runtime_error("Unsupported transport '" + transport + "'. Supported: udp, shm.");
  };

  auto validate_static_direction = [&canonical_transport](const std::string & direction_name,
                                                          const std::vector<std::string> & topics,
                                                          const std::vector<std::string> & types,
                                                          const std::vector<std::string> & transports,
                                                          const std::vector<int64_t> & qos_depths) {
    if (topics.size() != types.size()) {
      throw std::runtime_error(
          "Parameters '" + direction_name + ".topics' and '" + direction_name + ".topic_types' must have identical lengths.");
    }
    if (!transports.empty() && transports.size() != 1U && transports.size() != topics.size()) {
      throw std::runtime_error(
          "Parameter '" + direction_name + ".transports' must be empty, contain one value, or match '" + direction_name + ".topics'.");
    }
    if (!qos_depths.empty() && qos_depths.size() != 1U && qos_depths.size() != topics.size()) {
      throw std::runtime_error(
          "Parameter '" + direction_name + ".qos_depths' must be empty, contain one value, or match '" + direction_name + ".topics'.");
    }

    for (std::size_t idx = 0; idx < topics.size(); ++idx) {
      if (topics[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topics' contains an empty entry at index " + std::to_string(idx) + ".");
      }
      if (types[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topic_types' contains an empty entry at index " + std::to_string(idx) + ".");
      }
      const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
      if (qos_depth <= 0) {
        throw std::runtime_error("QoS depth must be greater than zero.");
      }
      if (!transports.empty()) {
        try {
          (void)canonical_transport(transports.size() == 1U ? transports.front() : transports[idx]);
        } catch (const std::runtime_error & ex) {
          throw std::runtime_error(
              "Parameter '" + direction_name + ".transports' contains unsupported value at index " + std::to_string(idx) + ": " + ex.what());
        }
      }
    }
  };

  auto validate_auto_direction = [&canonical_transport](const std::string & direction_name,
                                                        const std::vector<std::string> & types,
                                                        const std::vector<std::string> & transports,
                                                        const std::vector<int64_t> & qos_depths) {
    if (types.empty()) {
      throw std::runtime_error(
          "Auto-discovery for '" + direction_name + "' requires at least one entry in '" + direction_name + ".topic_types'.");
    }
    if (!transports.empty() && transports.size() != 1U && transports.size() != types.size()) {
      throw std::runtime_error(
          "Parameter '" + direction_name + ".transports' must be empty, contain one value, or match '" + direction_name + ".topic_types' in auto-discovery mode.");
    }
    if (!qos_depths.empty() && qos_depths.size() != 1U && qos_depths.size() != types.size()) {
      throw std::runtime_error(
          "Parameter '" + direction_name + ".qos_depths' must be empty, contain one value, or match '" + direction_name + ".topic_types' in auto-discovery mode.");
    }

    std::unordered_set<std::string> seen_types;
    for (std::size_t idx = 0; idx < types.size(); ++idx) {
      if (types[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topic_types' contains an empty entry at index " + std::to_string(idx) + ".");
      }
      if (!seen_types.insert(types[idx]).second) {
        throw std::runtime_error(
            "Parameter '" + direction_name + ".topic_types' contains duplicate type '" + types[idx] +
            "' in auto-discovery mode. Configure each type only once.");
      }
      const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
      if (qos_depth <= 0) {
        throw std::runtime_error("QoS depth must be greater than zero.");
      }
      if (!transports.empty()) {
        try {
          (void)canonical_transport(transports.size() == 1U ? transports.front() : transports[idx]);
        } catch (const std::runtime_error & ex) {
          throw std::runtime_error(
              "Parameter '" + direction_name + ".transports' contains unsupported value at index " + std::to_string(idx) + ": " + ex.what());
        }
      }
    }
  };

  dds2zenoh_auto_discovery_ = dds2zenoh_topics_.empty() && !dds2zenoh_topic_types_.empty();
  zenoh2dds_auto_discovery_ = zenoh2dds_topics_.empty() && !zenoh2dds_topic_types_.empty();
  auto_discovery_enabled_ = dds2zenoh_auto_discovery_ || zenoh2dds_auto_discovery_;

  if (dds2zenoh_auto_discovery_) {
    validate_auto_direction("dds2zenoh", dds2zenoh_topic_types_, dds2zenoh_transports_, dds2zenoh_qos_depths_);
  } else {
    validate_static_direction("dds2zenoh", dds2zenoh_topics_, dds2zenoh_topic_types_, dds2zenoh_transports_, dds2zenoh_qos_depths_);
  }

  if (zenoh2dds_auto_discovery_) {
    validate_auto_direction("zenoh2dds", zenoh2dds_topic_types_, zenoh2dds_transports_, zenoh2dds_qos_depths_);
  } else {
    validate_static_direction("zenoh2dds", zenoh2dds_topics_, zenoh2dds_topic_types_, zenoh2dds_transports_, zenoh2dds_qos_depths_);
  }

  const auto static_routes = dds2zenoh_topics_.size() + zenoh2dds_topics_.size();
  const auto auto_type_rules =
      (dds2zenoh_auto_discovery_ ? dds2zenoh_topic_types_.size() : 0U) +
      (zenoh2dds_auto_discovery_ ? zenoh2dds_topic_types_.size() : 0U);
  if (static_routes + auto_type_rules == 0U) {
    throw std::runtime_error("At least one route must be configured in 'dds2zenoh' or 'zenoh2dds'.");
  }
  if (static_routes > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    throw std::runtime_error("Too many static topic rules. Maximum is 65535.");
  }

  if (tx_port_ <= 0 || tx_port_ > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("Parameter 'tx_port' must be in range [1, 65535].");
  }
  if (rx_port_ <= 0 || rx_port_ > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("Parameter 'rx_port' must be in range [1, 65535].");
  }
  if (max_udp_payload_bytes_ <= static_cast<int>(sizeof(PacketHeader)) || max_udp_payload_bytes_ > static_cast<int>(kMaxUdpDatagramBytes)) {
    throw std::runtime_error(
        "Parameter 'max_udp_payload_bytes' must be in range (" + std::to_string(sizeof(PacketHeader)) + ", " +
        std::to_string(kMaxUdpDatagramBytes) + "].");
  }
  if (max_shm_message_bytes_ <= 0) {
    throw std::runtime_error("Parameter 'max_shm_message_bytes' must be greater than zero.");
  }
  if (shm_poll_interval_us_ <= 0) {
    throw std::runtime_error("Parameter 'shm_poll_interval_us' must be greater than zero.");
  }
  if (reassembly_timeout_ms_ <= 0) {
    throw std::runtime_error("Parameter 'reassembly_timeout_ms' must be greater than zero.");
  }
  if (shm_namespace_.empty()) {
    throw std::runtime_error("Parameter 'shm_namespace' must not be empty.");
  }

  RCLCPP_INFO(
      this->get_logger(),
      "Bridge config: role=%s remote_host=%s tx_port=%d rx_port=%d max_udp_payload_bytes=%d max_shm_message_bytes=%d shm_poll_interval_us=%d reassembly_timeout_ms=%d dds2zenoh_static=%zu zenoh2dds_static=%zu dds2zenoh_auto=%s zenoh2dds_auto=%s",
      bridge_role_.c_str(),
      remote_host_.c_str(),
      tx_port_,
      rx_port_,
      max_udp_payload_bytes_,
      max_shm_message_bytes_,
      shm_poll_interval_us_,
      reassembly_timeout_ms_,
      dds2zenoh_topics_.size(),
      zenoh2dds_topics_.size(),
      dds2zenoh_auto_discovery_ ? "true" : "false",
      zenoh2dds_auto_discovery_ ? "true" : "false");
}

std::size_t MiddlewareBridge::addChannelIfMissing(const bool is_dds2zenoh,
                                                  const std::string & topic_name,
                                                  const std::string & topic_type,
                                                  const std::string & transport,
                                                  const std::size_t qos_depth,
                                                  const bool from_auto_discovery,
                                                  bool * added) {
  auto canonical_transport = [](std::string value) -> std::string {
    std::transform(
        value.begin(),
        value.end(),
        value.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value == "udp") {
      return "udp";
    }
    if (value == "shm" || value == "shared_memory" || value == "shared-memory") {
      return "shm";
    }
    throw std::runtime_error("Unsupported transport '" + value + "'. Supported: udp, shm.");
  };

  const std::string canonical = canonical_transport(transport);
  const bool is_dds_role = bridge_role_ == "dds";
  const std::string subscribe_topic = is_dds2zenoh ? (is_dds_role ? topic_name : "") : (is_dds_role ? "" : topic_name);
  const std::string publish_topic = is_dds2zenoh ? (is_dds_role ? "" : topic_name) : (is_dds_role ? topic_name : "");
  const std::string channel_key = std::string(is_dds2zenoh ? "d2z|" : "z2d|") + topic_name + "|" + topic_type;

  std::lock_guard<std::mutex> lock(channels_mutex_);
  if (channel_keys_.count(channel_key) > 0U) {
    if (added != nullptr) {
      *added = false;
    }
    for (std::size_t idx = 0; idx < channels_.size(); ++idx) {
      const auto & channel = channels_[idx];
      if (channel.topic_type == topic_type &&
          channel.subscribe_topic == subscribe_topic &&
          channel.publish_topic == publish_topic) {
        return idx;
      }
    }
    throw std::runtime_error("Internal channel-key lookup mismatch for '" + channel_key + "'.");
  }

  if (channels_.size() >= static_cast<std::size_t>(kControlChannelId)) {
    throw std::runtime_error("Maximum number of channels reached (65535 including control channel).");
  }

  BridgeChannel channel;
  channel.subscribe_topic = subscribe_topic;
  channel.publish_topic = publish_topic;
  channel.topic_type = topic_type;
  channel.qos_depth = qos_depth;
  if (canonical == "udp") {
    channel.transport = TransportType::Udp;
    channel.transport_name = "udp";
    use_udp_transport_ = true;
  } else {
    channel.transport = TransportType::Shm;
    channel.transport_name = "shm";
    use_shm_transport_ = true;
  }

  const std::size_t channel_index = channels_.size();
  const auto qos = rclcpp::QoS(rclcpp::KeepLast(channel.qos_depth));
  if (!channel.publish_topic.empty()) {
    channel.publisher = this->create_generic_publisher(channel.publish_topic, channel.topic_type, qos);
  }
  if (!channel.subscribe_topic.empty()) {
    channel.subscriber = this->create_generic_subscription(
        channel.subscribe_topic,
        channel.topic_type,
        qos,
        [this, channel_index](std::shared_ptr<rclcpp::SerializedMessage> msg) {
          if (msg != nullptr) {
            this->forwardSerializedMessage(channel_index, *msg);
          }
        });
  }

  channels_.push_back(std::move(channel));
  channel_keys_.insert(channel_key);

  if (channels_.back().transport == TransportType::Shm && receiver_running_.load()) {
    const auto sanitizeName = [](const std::string & raw) -> std::string {
      std::string out;
      out.reserve(raw.size());
      for (const char c : raw) {
        if (std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_') {
          out.push_back(c);
        } else {
          out.push_back('_');
        }
      }
      if (out.empty()) {
        out = "default";
      }
      return out;
    };
    setupSharedMemoryChannel(channels_.back(), sanitizeName(shm_namespace_), channel_index);
  }

  std::string direction = "tx+rx";
  if (!channels_.back().subscribe_topic.empty() && channels_.back().publish_topic.empty()) {
    direction = "tx-only";
  } else if (channels_.back().subscribe_topic.empty() && !channels_.back().publish_topic.empty()) {
    direction = "rx-only";
  }

  RCLCPP_INFO(
      this->get_logger(),
      "Rule %zu (%s, transport=%s%s): subscribe '%s' -> bridge -> publish '%s' (%s, depth=%zu)",
      channel_index,
      direction.c_str(),
      channels_.back().transport_name.c_str(),
      from_auto_discovery ? ", auto" : "",
      channels_.back().subscribe_topic.c_str(),
      channels_.back().publish_topic.c_str(),
      channels_.back().topic_type.c_str(),
      channels_.back().qos_depth);
  if (added != nullptr) {
    *added = true;
  }
  return channel_index;
}

void MiddlewareBridge::setupBridgeChannels() {
  use_udp_transport_ = false;
  use_shm_transport_ = false;
  channel_keys_.clear();
  remote_channel_to_local_index_.clear();
  channels_.clear();
  channels_.reserve(dds2zenoh_topics_.size() + zenoh2dds_topics_.size());

  auto qosDepthForRule = [](const std::vector<int64_t> & qos_depths, std::size_t idx) -> std::size_t {
    const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
    return static_cast<std::size_t>(qos_depth);
  };
  auto transportForRule = [](const std::vector<std::string> & transports, std::size_t idx) -> std::string {
    if (transports.empty()) {
      return "udp";
    }
    return transports.size() == 1U ? transports.front() : transports[idx];
  };

  for (std::size_t idx = 0; idx < dds2zenoh_topics_.size(); ++idx) {
    (void)addChannelIfMissing(
        true,
        dds2zenoh_topics_[idx],
        dds2zenoh_topic_types_[idx],
        transportForRule(dds2zenoh_transports_, idx),
        qosDepthForRule(dds2zenoh_qos_depths_, idx),
        false);
  }
  for (std::size_t idx = 0; idx < zenoh2dds_topics_.size(); ++idx) {
    (void)addChannelIfMissing(
        false,
        zenoh2dds_topics_[idx],
        zenoh2dds_topic_types_[idx],
        transportForRule(zenoh2dds_transports_, idx),
        qosDepthForRule(zenoh2dds_qos_depths_, idx),
        false);
  }

  auto mayUseUdp = [](const std::vector<std::string> & transports) -> bool {
    if (transports.empty()) {
      return true;
    }
    for (const auto & raw : transports) {
      std::string value = raw;
      std::transform(
          value.begin(),
          value.end(),
          value.begin(),
          [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      if (value == "udp") {
        return true;
      }
    }
    return false;
  };
  auto mayUseShm = [](const std::vector<std::string> & transports) -> bool {
    for (const auto & raw : transports) {
      std::string value = raw;
      std::transform(
          value.begin(),
          value.end(),
          value.begin(),
          [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      if (value == "shm" || value == "shared_memory" || value == "shared-memory") {
        return true;
      }
    }
    return false;
  };

  if (dds2zenoh_auto_discovery_) {
    use_udp_transport_ = use_udp_transport_ || mayUseUdp(dds2zenoh_transports_);
    use_shm_transport_ = use_shm_transport_ || mayUseShm(dds2zenoh_transports_);
  }
  if (zenoh2dds_auto_discovery_) {
    use_udp_transport_ = use_udp_transport_ || mayUseUdp(zenoh2dds_transports_);
    use_shm_transport_ = use_shm_transport_ || mayUseShm(zenoh2dds_transports_);
  }

  if (auto_discovery_enabled_) {
    use_udp_transport_ = true;
  }
}

void MiddlewareBridge::setupSockets() {
  tx_socket_fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (tx_socket_fd_ < 0) {
    throw std::runtime_error("Failed to create TX socket: " + std::string(std::strerror(errno)));
  }

  rx_socket_fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (rx_socket_fd_ < 0) {
    throw std::runtime_error("Failed to create RX socket: " + std::string(std::strerror(errno)));
  }

  const int reuse = 1;
  if (::setsockopt(rx_socket_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
    RCLCPP_WARN(this->get_logger(), "Failed to set SO_REUSEADDR on RX socket: %s", std::strerror(errno));
  }

  if (socket_buffer_bytes_ > 0) {
    if (::setsockopt(rx_socket_fd_, SOL_SOCKET, SO_RCVBUF, &socket_buffer_bytes_, sizeof(socket_buffer_bytes_)) != 0) {
      RCLCPP_WARN(this->get_logger(), "Failed to set SO_RCVBUF: %s", std::strerror(errno));
    }
    if (::setsockopt(tx_socket_fd_, SOL_SOCKET, SO_SNDBUF, &socket_buffer_bytes_, sizeof(socket_buffer_bytes_)) != 0) {
      RCLCPP_WARN(this->get_logger(), "Failed to set SO_SNDBUF: %s", std::strerror(errno));
    }
  }

  std::memset(&tx_address_, 0, sizeof(tx_address_));
  tx_address_.sin_family = AF_INET;
  tx_address_.sin_port = htons(static_cast<uint16_t>(tx_port_));
  if (::inet_pton(AF_INET, remote_host_.c_str(), &tx_address_.sin_addr) != 1) {
    throw std::runtime_error("Failed to parse remote_host '" + remote_host_ + "' as IPv4 address.");
  }

  struct sockaddr_in rx_address {};
  rx_address.sin_family = AF_INET;
  rx_address.sin_port = htons(static_cast<uint16_t>(rx_port_));
  rx_address.sin_addr.s_addr = htonl(INADDR_ANY);
  if (::bind(rx_socket_fd_, reinterpret_cast<struct sockaddr *>(&rx_address), sizeof(rx_address)) != 0) {
    throw std::runtime_error("Failed to bind RX socket on port " + std::to_string(rx_port_) + ": " + std::strerror(errno));
  }
}

void MiddlewareBridge::setupSharedMemoryChannel(BridgeChannel & channel, const std::string & ns, const std::size_t channel_index) {
  if (channel.shm_fd >= 0 && channel.shm_mapping != nullptr) {
    return;
  }

  const std::size_t mapping_size = sizeof(ShmChannelHeader) + static_cast<std::size_t>(max_shm_message_bytes_);
  const std::string shm_name = "/mb_" + ns + "_ch_" + std::to_string(channel_index);
  const int fd = ::shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    throw std::runtime_error("Failed to open shared memory '" + shm_name + "': " + std::strerror(errno));
  }

  if (::ftruncate(fd, static_cast<off_t>(mapping_size)) != 0) {
    const std::string error = "Failed to resize shared memory '" + shm_name + "': " + std::strerror(errno);
    ::close(fd);
    throw std::runtime_error(error);
  }

  void * mapping = ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mapping == MAP_FAILED) {
    const std::string error = "Failed to map shared memory '" + shm_name + "': " + std::strerror(errno);
    ::close(fd);
    throw std::runtime_error(error);
  }

  auto * header = reinterpret_cast<ShmChannelHeader *>(mapping);
  auto * payload = reinterpret_cast<std::uint8_t *>(mapping) + sizeof(ShmChannelHeader);
  const auto expected_capacity = static_cast<std::uint32_t>(max_shm_message_bytes_);
  const auto existing_magic = header->magic.load(std::memory_order_acquire);
  const auto existing_capacity = header->capacity_bytes.load(std::memory_order_acquire);
  if (existing_magic != kShmMagic || existing_capacity != expected_capacity) {
    header->capacity_bytes.store(expected_capacity, std::memory_order_relaxed);
    header->sequence.store(0U, std::memory_order_relaxed);
    header->payload_size.store(0U, std::memory_order_relaxed);
    header->reserved.store(0U, std::memory_order_relaxed);
    if (expected_capacity > 0U) {
      std::memset(payload, 0, expected_capacity);
    }
    header->magic.store(kShmMagic, std::memory_order_release);
  }

  if (header->capacity_bytes.load(std::memory_order_acquire) != expected_capacity) {
    ::munmap(mapping, mapping_size);
    ::close(fd);
    throw std::runtime_error("Shared memory capacity mismatch for '" + shm_name + "'.");
  }

  channel.shm_fd = fd;
  channel.shm_mapping = mapping;
  channel.shm_mapping_size = mapping_size;
  channel.shm_header = header;
  channel.shm_payload = payload;
  channel.shm_last_sequence = header->sequence.load(std::memory_order_acquire);
  channel.shm_read_buffer.reserve(static_cast<std::size_t>(max_shm_message_bytes_));

  RCLCPP_INFO(this->get_logger(), "Rule %zu uses shared memory '%s' (capacity=%d bytes)", channel_index, shm_name.c_str(), max_shm_message_bytes_);
}

void MiddlewareBridge::setupSharedMemoryChannels() {
  const auto sanitizeName = [](const std::string & raw) -> std::string {
    std::string out;
    out.reserve(raw.size());
    for (const char c : raw) {
      if (std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_') {
        out.push_back(c);
      } else {
        out.push_back('_');
      }
    }
    if (out.empty()) {
      out = "default";
    }
    return out;
  };

  const std::string ns = sanitizeName(shm_namespace_);
  std::lock_guard<std::mutex> lock(channels_mutex_);
  for (std::size_t channel_index = 0; channel_index < channels_.size(); ++channel_index) {
    auto & channel = channels_[channel_index];
    if (channel.transport != TransportType::Shm) {
      continue;
    }
    setupSharedMemoryChannel(channel, ns, channel_index);
  }
}

void MiddlewareBridge::runAutoDiscoveryScan() {
  if (!auto_discovery_enabled_) {
    return;
  }

  const auto topic_graph = this->get_topic_names_and_types();
  std::size_t added_count = 0;

  auto qosDepthForRule = [](const std::vector<int64_t> & qos_depths, std::size_t idx) -> std::size_t {
    const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
    return static_cast<std::size_t>(qos_depth);
  };
  auto transportForRule = [](const std::vector<std::string> & transports, std::size_t idx) -> std::string {
    if (transports.empty()) {
      return "udp";
    }
    return transports.size() == 1U ? transports.front() : transports[idx];
  };

  auto scanDirection = [&](const bool enabled,
                           const bool is_dds2zenoh,
                           const std::string & direction_name,
                           const std::vector<std::string> & topic_types,
                           const std::vector<std::string> & transports,
                           const std::vector<int64_t> & qos_depths) {
    if (!enabled) {
      return;
    }

    std::unordered_set<std::string> seen_topics_this_scan;
    for (std::size_t rule_index = 0; rule_index < topic_types.size(); ++rule_index) {
      const std::string & type_name = topic_types[rule_index];
      std::vector<std::string> matched_topics;
      for (const auto & [topic_name, topic_type_list] : topic_graph) {
        if (std::find(topic_type_list.begin(), topic_type_list.end(), type_name) != topic_type_list.end()) {
          matched_topics.push_back(topic_name);
        }
      }
      std::sort(matched_topics.begin(), matched_topics.end());

      const auto transport = transportForRule(transports, rule_index);
      const auto qos_depth = qosDepthForRule(qos_depths, rule_index);
      for (const auto & topic_name : matched_topics) {
        if (!seen_topics_this_scan.insert(topic_name).second) {
          RCLCPP_WARN(
              this->get_logger(),
              "Skipping auto-discovered topic '%s' in %s because it matched multiple configured types.",
              topic_name.c_str(),
              direction_name.c_str());
          continue;
        }
        bool added = false;
        const auto channel_index = addChannelIfMissing(is_dds2zenoh, topic_name, type_name, transport, qos_depth, true, &added);
        announceAutoDiscoveredChannel(
            static_cast<std::uint16_t>(channel_index),
            is_dds2zenoh,
            topic_name,
            type_name,
            transport,
            qos_depth);
        if (added) {
          added_count += 1U;
        }
      }
    }
  };

  scanDirection(dds2zenoh_auto_discovery_, true, "dds2zenoh", dds2zenoh_topic_types_, dds2zenoh_transports_, dds2zenoh_qos_depths_);
  scanDirection(zenoh2dds_auto_discovery_, false, "zenoh2dds", zenoh2dds_topic_types_, zenoh2dds_transports_, zenoh2dds_qos_depths_);

  if (added_count > 0U) {
    RCLCPP_INFO(this->get_logger(), "Auto-discovery added %zu new channel(s).", added_count);
  }
}

void MiddlewareBridge::sendUdpPayload(const std::uint16_t channel_id, const std::uint8_t * payload, const std::size_t payload_size) {
  if (tx_socket_fd_ < 0) {
    return;
  }

  if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    RCLCPP_WARN(this->get_logger(), "Dropping oversized UDP payload (%zu bytes).", payload_size);
    return;
  }

  const std::size_t header_size = sizeof(PacketHeader);
  const std::size_t max_fragment_payload_size = static_cast<std::size_t>(max_udp_payload_bytes_) - header_size;
  const std::size_t fragment_count =
      std::max<std::size_t>(1, (payload_size + max_fragment_payload_size - 1) / max_fragment_payload_size);
  if (fragment_count > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    RCLCPP_WARN(
        this->get_logger(),
        "Dropping UDP payload because required fragment count %zu exceeds limit %u.",
        fragment_count,
        static_cast<unsigned int>(std::numeric_limits<std::uint16_t>::max()));
    return;
  }

  const std::uint32_t message_id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(send_mutex_);
  for (std::size_t fragment_index = 0; fragment_index < fragment_count; ++fragment_index) {
    const std::size_t fragment_offset = fragment_index * max_fragment_payload_size;
    const std::size_t fragment_payload_size =
        fragment_offset < payload_size ? std::min(max_fragment_payload_size, payload_size - fragment_offset) : 0U;

    PacketHeader wire_header {};
    wire_header.magic = htonl(kPacketMagic);
    wire_header.version = htons(kPacketVersion);
    wire_header.channel_index = htons(channel_id);
    wire_header.message_id = htonl(message_id);
    wire_header.fragment_index = htons(static_cast<std::uint16_t>(fragment_index));
    wire_header.fragment_count = htons(static_cast<std::uint16_t>(fragment_count));
    wire_header.total_payload_size = htonl(static_cast<std::uint32_t>(payload_size));
    wire_header.fragment_offset = htonl(static_cast<std::uint32_t>(fragment_offset));
    wire_header.fragment_payload_size = htonl(static_cast<std::uint32_t>(fragment_payload_size));

    const std::size_t packet_size = header_size + fragment_payload_size;
    std::vector<std::uint8_t> packet(packet_size);
    std::memcpy(packet.data(), &wire_header, header_size);
    if (fragment_payload_size > 0U) {
      std::memcpy(packet.data() + header_size, payload + fragment_offset, fragment_payload_size);
    }

    const ssize_t bytes_sent = ::sendto(
        tx_socket_fd_,
        packet.data(),
        packet.size(),
        0,
        reinterpret_cast<struct sockaddr *>(&tx_address_),
        sizeof(tx_address_));
    if (bytes_sent < 0 || static_cast<std::size_t>(bytes_sent) != packet.size()) {
      RCLCPP_WARN(this->get_logger(), "Failed UDP send for channel %u: %s", channel_id, std::strerror(errno));
      return;
    }
  }
}

void MiddlewareBridge::announceAutoDiscoveredChannel(const std::uint16_t channel_id,
                                                     const bool is_dds2zenoh,
                                                     const std::string & topic_name,
                                                     const std::string & topic_type,
                                                     const std::string & transport,
                                                     const std::size_t qos_depth) {
  if (tx_socket_fd_ < 0) {
    return;
  }

  std::string normalized_transport = transport;
  std::transform(
      normalized_transport.begin(),
      normalized_transport.end(),
      normalized_transport.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (normalized_transport == "shared_memory" || normalized_transport == "shared-memory") {
    normalized_transport = "shm";
  }

  const std::string payload = "A1|" + std::to_string(channel_id) + "|" + (is_dds2zenoh ? "d2z" : "z2d") + "|" +
                              normalized_transport + "|" + std::to_string(qos_depth) + "|" + topic_name + "|" + topic_type;
  sendUdpPayload(kControlChannelId, reinterpret_cast<const std::uint8_t *>(payload.data()), payload.size());
}

void MiddlewareBridge::handleAutoDiscoveryAnnouncement(const std::uint8_t * payload, const std::size_t payload_size) {
  if (payload == nullptr || payload_size == 0U) {
    return;
  }

  const std::string message(reinterpret_cast<const char *>(payload), payload_size);
  std::vector<std::string> fields;
  fields.reserve(8);
  std::size_t begin = 0U;
  while (begin <= message.size()) {
    const auto sep = message.find('|', begin);
    if (sep == std::string::npos) {
      fields.push_back(message.substr(begin));
      break;
    }
    fields.push_back(message.substr(begin, sep - begin));
    begin = sep + 1U;
  }

  if (fields.size() != 7U || fields[0] != "A1") {
    return;
  }

  std::uint16_t remote_channel_index = 0;
  std::size_t qos_depth = 10;
  try {
    const auto parsed_channel_index = std::stoul(fields[1]);
    if (parsed_channel_index >= static_cast<unsigned long>(kControlChannelId)) {
      return;
    }
    remote_channel_index = static_cast<std::uint16_t>(parsed_channel_index);
    qos_depth = static_cast<std::size_t>(std::stoul(fields[4]));
  } catch (...) {
    return;
  }
  if (qos_depth == 0U) {
    return;
  }

  const bool is_dds2zenoh = fields[2] == "d2z";
  if (!is_dds2zenoh && fields[2] != "z2d") {
    return;
  }

  const std::string & transport = fields[3];
  const std::string & topic_name = fields[5];
  const std::string & topic_type = fields[6];
  if (topic_name.empty() || topic_type.empty()) {
    return;
  }

  bool added = false;
  const auto local_channel_index = addChannelIfMissing(is_dds2zenoh, topic_name, topic_type, transport, qos_depth, true, &added);
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    remote_channel_to_local_index_[remote_channel_index] = local_channel_index;
  }
}

void MiddlewareBridge::receiverLoop() {
  struct ReassemblyState {
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> fragment_received;
    std::size_t fragments_received = 0;
    std::size_t bytes_received = 0;
    std::chrono::steady_clock::time_point last_update;
  };

  const std::size_t header_size = sizeof(PacketHeader);
  const std::size_t receive_buffer_size = static_cast<std::size_t>(std::max(4096, std::max(socket_buffer_bytes_, max_udp_payload_bytes_)));
  std::vector<std::uint8_t> receive_buffer(receive_buffer_size);
  std::unordered_map<std::uint64_t, ReassemblyState> reassembly_states;
  std::size_t received_datagrams = 0;

  auto clear_stale_reassemblies = [&]() {
    const auto now = std::chrono::steady_clock::now();
    for (auto it = reassembly_states.begin(); it != reassembly_states.end();) {
      const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.last_update).count();
      if (age_ms > reassembly_timeout_ms_) {
        it = reassembly_states.erase(it);
      } else {
        ++it;
      }
    }
  };

  auto publish_serialized = [this](std::size_t channel_index, const std::uint8_t * data, std::size_t size) {
    rclcpp::GenericPublisher::SharedPtr publisher;
    {
      std::lock_guard<std::mutex> lock(channels_mutex_);
      if (channel_index >= channels_.size()) {
        return;
      }
      auto & channel = channels_[channel_index];
      if (!channel.publisher || channel.transport != TransportType::Udp) {
        return;
      }
      publisher = channel.publisher;
    }

    rclcpp::SerializedMessage serialized_message(size);
    auto & rcl_serialized = serialized_message.get_rcl_serialized_message();
    if (size > 0U) {
      std::memcpy(rcl_serialized.buffer, data, size);
    }
    rcl_serialized.buffer_length = size;
    publisher->publish(serialized_message);
  };

  struct sockaddr_in source_address {};
  socklen_t source_length = sizeof(source_address);

  while (receiver_running_.load() && rclcpp::ok()) {
    if ((++received_datagrams % 64U) == 0U) {
      clear_stale_reassemblies();
    }

    source_length = sizeof(source_address);
    const ssize_t received_bytes =
        ::recvfrom(rx_socket_fd_, receive_buffer.data(), receive_buffer.size(), 0, reinterpret_cast<struct sockaddr *>(&source_address), &source_length);

    if (received_bytes < 0) {
      if (!receiver_running_.load()) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      RCLCPP_WARN(this->get_logger(), "recvfrom failed: %s", std::strerror(errno));
      continue;
    }

    if (received_bytes < static_cast<ssize_t>(header_size)) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet smaller than header (%zd bytes)", received_bytes);
      continue;
    }

    PacketHeader wire_header {};
    std::memcpy(&wire_header, receive_buffer.data(), header_size);
    const auto magic = ntohl(wire_header.magic);
    const auto version = ntohs(wire_header.version);
    const auto channel_index = ntohs(wire_header.channel_index);
    const auto message_id = ntohl(wire_header.message_id);
    const auto fragment_index = ntohs(wire_header.fragment_index);
    const auto fragment_count = ntohs(wire_header.fragment_count);
    const auto total_payload_size = ntohl(wire_header.total_payload_size);
    const auto fragment_offset = ntohl(wire_header.fragment_offset);
    const auto fragment_payload_size = ntohl(wire_header.fragment_payload_size);

    if (magic != kPacketMagic || version != kPacketVersion) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with unsupported magic/version.");
      continue;
    }

    if (channel_index == kControlChannelId) {
      if (fragment_count != 1U || fragment_index != 0U || fragment_offset != 0U || total_payload_size != fragment_payload_size) {
        RCLCPP_WARN(this->get_logger(), "Dropped malformed control packet.");
        continue;
      }
      const auto expected_size = header_size + static_cast<std::size_t>(fragment_payload_size);
      if (static_cast<std::size_t>(received_bytes) != expected_size) {
        RCLCPP_WARN(this->get_logger(), "Dropped control packet with invalid size.");
        continue;
      }
      handleAutoDiscoveryAnnouncement(receive_buffer.data() + header_size, total_payload_size);
      continue;
    }

    bool is_valid_udp_channel = false;
    std::size_t local_channel_index = channel_index;
    {
      std::lock_guard<std::mutex> lock(channels_mutex_);
      const auto map_it = remote_channel_to_local_index_.find(channel_index);
      if (map_it != remote_channel_to_local_index_.end()) {
        local_channel_index = map_it->second;
      }
      if (local_channel_index < channels_.size() && channels_[local_channel_index].publisher &&
          channels_[local_channel_index].transport == TransportType::Udp) {
        is_valid_udp_channel = true;
      }
    }
    if (!is_valid_udp_channel) {
      continue;
    }

    if (fragment_count == 0U || fragment_index >= fragment_count) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with invalid fragment metadata.");
      continue;
    }

    const std::uint64_t fragment_end_offset = static_cast<std::uint64_t>(fragment_offset) + static_cast<std::uint64_t>(fragment_payload_size);
    if (fragment_end_offset > static_cast<std::uint64_t>(total_payload_size)) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with invalid payload bounds.");
      continue;
    }

    const auto expected_size = header_size + static_cast<std::size_t>(fragment_payload_size);
    if (static_cast<std::size_t>(received_bytes) != expected_size) {
      RCLCPP_WARN(
          this->get_logger(),
          "Dropped packet with invalid fragment size: declared=%u bytes total_received=%zd",
          fragment_payload_size,
          received_bytes);
      continue;
    }

    const std::uint8_t * fragment_data = receive_buffer.data() + header_size;

    if (fragment_count == 1U) {
      if (fragment_index != 0U || fragment_offset != 0U || total_payload_size != fragment_payload_size) {
        RCLCPP_WARN(this->get_logger(), "Dropped malformed single-fragment packet.");
        continue;
      }
      publish_serialized(local_channel_index, fragment_data, total_payload_size);
      continue;
    }

    const std::uint64_t reassembly_key = (static_cast<std::uint64_t>(channel_index) << 32U) | static_cast<std::uint64_t>(message_id);
    auto & state = reassembly_states[reassembly_key];
    if (state.payload.size() != total_payload_size || state.fragment_received.size() != fragment_count) {
      state.payload.assign(total_payload_size, 0U);
      state.fragment_received.assign(fragment_count, 0U);
      state.fragments_received = 0U;
      state.bytes_received = 0U;
    }
    state.last_update = std::chrono::steady_clock::now();

    if (state.fragment_received[fragment_index] == 0U) {
      if (fragment_payload_size > 0U) {
        std::memcpy(
            state.payload.data() + fragment_offset,
            fragment_data,
            fragment_payload_size);
      }
      state.fragment_received[fragment_index] = 1U;
      state.fragments_received += 1U;
      state.bytes_received += fragment_payload_size;
    }

    if (state.fragments_received == state.fragment_received.size() && state.bytes_received == state.payload.size()) {
      publish_serialized(local_channel_index, state.payload.data(), state.payload.size());
      reassembly_states.erase(reassembly_key);
    }
  }
}

void MiddlewareBridge::shmReceiverLoop() {
  struct PendingPublish {
    rclcpp::GenericPublisher::SharedPtr publisher;
    std::vector<std::uint8_t> payload;
  };

  while (receiver_running_.load() && rclcpp::ok()) {
    std::vector<PendingPublish> pending_publishes;
    {
      std::lock_guard<std::mutex> lock(channels_mutex_);
      for (std::size_t channel_index = 0; channel_index < channels_.size(); ++channel_index) {
        auto & channel = channels_[channel_index];
        if (channel.transport != TransportType::Shm || !channel.publisher || channel.shm_header == nullptr || channel.shm_payload == nullptr) {
          continue;
        }

        const std::uint64_t sequence_begin = channel.shm_header->sequence.load(std::memory_order_acquire);
        if ((sequence_begin & 1U) != 0U || sequence_begin == channel.shm_last_sequence) {
          continue;
        }

        const std::uint32_t payload_size = channel.shm_header->payload_size.load(std::memory_order_acquire);
        if (payload_size > static_cast<std::uint32_t>(max_shm_message_bytes_)) {
          RCLCPP_WARN(
              this->get_logger(),
              "Dropping SHM message on channel %zu because payload_size=%u exceeds max_shm_message_bytes=%d.",
              channel_index,
              payload_size,
              max_shm_message_bytes_);
          channel.shm_last_sequence = sequence_begin;
          continue;
        }

        PendingPublish pending;
        pending.publisher = channel.publisher;
        pending.payload.resize(payload_size);
        if (payload_size > 0U) {
          std::memcpy(pending.payload.data(), channel.shm_payload, payload_size);
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        const std::uint64_t sequence_end = channel.shm_header->sequence.load(std::memory_order_acquire);
        if (sequence_begin != sequence_end || (sequence_end & 1U) != 0U) {
          continue;
        }

        if (sequence_end != channel.shm_last_sequence) {
          channel.shm_last_sequence = sequence_end;
          pending_publishes.push_back(std::move(pending));
        }
      }
    }

    for (auto & pending : pending_publishes) {
      rclcpp::SerializedMessage serialized_message(pending.payload.size());
      auto & rcl_serialized = serialized_message.get_rcl_serialized_message();
      if (!pending.payload.empty()) {
        std::memcpy(rcl_serialized.buffer, pending.payload.data(), pending.payload.size());
      }
      rcl_serialized.buffer_length = pending.payload.size();
      pending.publisher->publish(serialized_message);
    }

    std::this_thread::sleep_for(std::chrono::microseconds(shm_poll_interval_us_));
  }
}

void MiddlewareBridge::forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage & message) {
  TransportType transport = TransportType::Udp;
  ShmChannelHeader * shm_header = nullptr;
  std::uint8_t * shm_payload = nullptr;

  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    if (channel_index >= channels_.size()) {
      return;
    }
    transport = channels_[channel_index].transport;
    shm_header = channels_[channel_index].shm_header;
    shm_payload = channels_[channel_index].shm_payload;
  }

  const auto & rcl_serialized = message.get_rcl_serialized_message();
  const std::size_t payload_size = rcl_serialized.buffer_length;
  if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    RCLCPP_WARN(this->get_logger(), "Dropping oversized serialized message (%zu bytes).", payload_size);
    return;
  }

  if (transport == TransportType::Shm) {
    if (shm_header == nullptr || shm_payload == nullptr) {
      RCLCPP_WARN(this->get_logger(), "Dropping SHM message on channel %zu because SHM channel is not initialized.", channel_index);
      return;
    }
    if (payload_size > static_cast<std::size_t>(max_shm_message_bytes_)) {
      RCLCPP_WARN(
          this->get_logger(),
          "Dropping SHM message on channel %zu because size=%zu exceeds max_shm_message_bytes=%d.",
          channel_index,
          payload_size,
          max_shm_message_bytes_);
      return;
    }

    std::uint64_t sequence = shm_header->sequence.load(std::memory_order_relaxed);
    if ((sequence & 1U) != 0U) {
      sequence += 1U;
    }
    shm_header->sequence.store(sequence + 1U, std::memory_order_release);
    shm_header->payload_size.store(static_cast<std::uint32_t>(payload_size), std::memory_order_relaxed);
    if (payload_size > 0U) {
      std::memcpy(shm_payload, rcl_serialized.buffer, payload_size);
    }
    std::atomic_thread_fence(std::memory_order_release);
    shm_header->sequence.store(sequence + 2U, std::memory_order_release);
    return;
  }

  if (tx_socket_fd_ < 0) {
    RCLCPP_WARN(this->get_logger(), "Dropping UDP message on channel %zu because UDP socket is not initialized.", channel_index);
    return;
  }
  sendUdpPayload(static_cast<std::uint16_t>(channel_index), rcl_serialized.buffer, payload_size);
}

void MiddlewareBridge::stopBackgroundThreads() {
  auto_discovery_timer_.reset();
  receiver_running_.store(false);

  if (rx_socket_fd_ >= 0) {
    ::shutdown(rx_socket_fd_, SHUT_RDWR);
  }
  if (tx_socket_fd_ >= 0) {
    ::shutdown(tx_socket_fd_, SHUT_RDWR);
  }

  if (udp_receiver_thread_.joinable()) {
    udp_receiver_thread_.join();
  }
  if (shm_receiver_thread_.joinable()) {
    shm_receiver_thread_.join();
  }

  if (rx_socket_fd_ >= 0) {
    ::close(rx_socket_fd_);
    rx_socket_fd_ = -1;
  }
  if (tx_socket_fd_ >= 0) {
    ::close(tx_socket_fd_);
    tx_socket_fd_ = -1;
  }
}

void MiddlewareBridge::closeSharedMemoryChannels() {
  std::lock_guard<std::mutex> lock(channels_mutex_);
  for (auto & channel : channels_) {
    if (channel.shm_mapping != nullptr && channel.shm_mapping_size > 0U) {
      ::munmap(channel.shm_mapping, channel.shm_mapping_size);
      channel.shm_mapping = nullptr;
      channel.shm_mapping_size = 0U;
      channel.shm_header = nullptr;
      channel.shm_payload = nullptr;
    }
    if (channel.shm_fd >= 0) {
      ::close(channel.shm_fd);
      channel.shm_fd = -1;
    }
  }
}

}  // namespace middleware_bridge

int main(int argc, char * argv[]) {
  rclcpp::init(argc, argv);

  auto node = std::make_shared<middleware_bridge::MiddlewareBridge>();
  rclcpp::executors::MultiThreadedExecutor executor(rclcpp::ExecutorOptions(), node->num_threads_);
  executor.add_node(node);
  executor.spin();

  rclcpp::shutdown();
  return 0;
}
