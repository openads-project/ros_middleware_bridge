// Copyright Institute for Automotive Engineering (ika), RWTH Aachen University
// SPDX-License-Identifier: Apache-2.0

#include <arpa/inet.h>
#include <fcntl.h>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <rclcpp/serialization.hpp>
#include <ros_middleware_bridge/ros_middleware_bridge.hpp>

namespace ros_middleware_bridge {

namespace {

bool isTfStaticTopic(const std::string& topic_name) {
  constexpr const char* suffix = "/tf_static";
  constexpr std::size_t suffix_length = 10U;
  return topic_name == "tf_static" || (topic_name.size() >= suffix_length &&
                                       topic_name.compare(topic_name.size() - suffix_length, suffix_length, suffix) == 0);
}

bool isTfTopic(const std::string& topic_name) {
  constexpr const char* suffix = "/tf";
  constexpr std::size_t suffix_length = 3U;
  return topic_name == "tf" || (topic_name.size() >= suffix_length &&
                                topic_name.compare(topic_name.size() - suffix_length, suffix_length, suffix) == 0);
}

}  // namespace

MiddlewareBridge::MiddlewareBridge() : Node("ros_middleware_bridge") {
  try {
    this->declareAndLoadParameters();
    setupBridgeChannels();

    if (use_udp_transport_) {
      setupSockets();
    }

    if (auto_discovery_enabled_ && auto_discovery_wait_ms_ > 0) {
      RCLCPP_INFO(this->get_logger(), "Waiting %d ms before initial auto-discovery scan.", auto_discovery_wait_ms_);
      std::this_thread::sleep_for(std::chrono::milliseconds(auto_discovery_wait_ms_));
    }
    refreshLocalSourceQos();
    if (use_udp_transport_) {
      announceStaticSourceChannels();
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

    if ((auto_discovery_enabled_ || use_udp_transport_) && auto_discovery_poll_ms_ > 0) {
      auto_discovery_timer_ = this->create_wall_timer(std::chrono::milliseconds(auto_discovery_poll_ms_), [this]() {
        this->refreshLocalSourceQos();
        if (this->use_udp_transport_) {
          this->announceStaticSourceChannels();
        }
        this->runAutoDiscoveryScan();
      });
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
  auto declare_string_array_parameter = [this](const std::string& name, const std::vector<std::string>& default_value,
                                               const std::string& description,
                                               bool fallback_to_empty_on_unset) -> std::vector<std::string> {
    auto value = default_value;
    try {
      this->declareAndLoadParameter(name, value, description);
      return value;
    } catch (const rclcpp::exceptions::InvalidParameterValueException& ex) {
      const std::string message = ex.what();
      if (fallback_to_empty_on_unset && message.find("No parameter value set") != std::string::npos) {
        RCLCPP_WARN(this->get_logger(), "Parameter '%s' is set without value. Falling back to empty list.", name.c_str());
        return {};
      }
      throw;
    }
  };

  this->declareAndLoadParameter("num_threads", num_threads_, "Number of threads used by the rclcpp MultiThreadedExecutor",
                                false, false, false, 1.0, 128.0);
  this->declareAndLoadParameter("bridge_side", bridge_side_, "Bridge side selector: a or b");
  this->declareAndLoadParameter("remote_host", remote_host_, "IPv4 destination used for UDP sends");
  this->declareAndLoadParameter("shm_namespace", shm_namespace_, "Namespace prefix used for shared-memory channel names");
  this->declareAndLoadParameter("tx_port", tx_port_, "UDP transmit port", false, false, false, 1.0, 65535.0);
  this->declareAndLoadParameter("rx_port", rx_port_, "UDP receive port", false, false, false, 1.0, 65535.0);
  this->declareAndLoadParameter("socket_buffer_bytes", socket_buffer_bytes_, "UDP socket send/receive buffer size in bytes",
                                false, false, false, 1.0, static_cast<double>(std::numeric_limits<int>::max()));
  this->declareAndLoadParameter(
      "max_udp_payload_bytes", max_udp_payload_bytes_,
      "Maximum UDP datagram payload per fragment, including the bridge fragment header", false, false, false,
      static_cast<double>(sizeof(PacketHeader) + 1U), static_cast<double>(kMaxUdpDatagramBytes));
  this->declareAndLoadParameter("max_shm_message_bytes", max_shm_message_bytes_,
                                "Maximum message size per shared-memory channel in bytes", false, false, false, 1.0,
                                static_cast<double>(std::numeric_limits<int>::max()));
  this->declareAndLoadParameter("shm_poll_interval_us", shm_poll_interval_us_,
                                "Poll interval for the shared-memory receiver loop in microseconds", false, false, false, 1.0,
                                static_cast<double>(std::numeric_limits<int>::max()));
  this->declareAndLoadParameter("reassembly_timeout_ms", reassembly_timeout_ms_,
                                "Timeout for incomplete UDP fragment reassembly in milliseconds", false, false, false, 1.0,
                                static_cast<double>(std::numeric_limits<int>::max()));
  this->declareAndLoadParameter(
      "auto_discovery_wait_ms", auto_discovery_wait_ms_,
      "Optional wait before the initial auto-discovery scan in milliseconds; 0 disables the wait", false, false, false, 0.0,
      static_cast<double>(std::numeric_limits<int>::max()));
  this->declareAndLoadParameter("auto_discovery_poll_ms", auto_discovery_poll_ms_,
                                "Poll interval for runtime auto-discovery scans in milliseconds", false, false, false, 1.0,
                                static_cast<double>(std::numeric_limits<int>::max()));

  auto canonical_bridge_side = [](std::string value, const std::string& parameter_name) -> std::string {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    std::replace(value.begin(), value.end(), '-', '_');

    if (value == "a" || value == "side_a" || value == "sidea") {
      return "a";
    }
    if (value == "b" || value == "side_b" || value == "sideb") {
      return "b";
    }

    throw std::runtime_error("Parameter '" + parameter_name + "' must resolve to side A or side B (e.g. a, side_a, b, side_b).");
  };

  bridge_side_ = canonical_bridge_side(bridge_side_, "bridge_side");

  struct DirectionParameters {
    std::vector<std::string> topics;
    std::vector<std::string> topic_types;
    std::vector<std::string> transports;
    std::vector<int64_t> qos_depths;
  };

  DirectionParameters side_a2b_config{
      declare_string_array_parameter("side_a2b.topics", std::vector<std::string>{},
                                     "Side A to side B topics; explicit topics in static mode or __auto__ for auto-discovery",
                                     true),
      declare_string_array_parameter("side_a2b.topic_types", std::vector<std::string>{},
                                     "Side A to side B message types; one type per topic or auto-discovery type filters",
                                     false),
      declare_string_array_parameter("side_a2b.transports", std::vector<std::string>{},
                                     "Side A to side B transport selection per topic/type: udp or shm", false),
      side_a2b_qos_depths_};
  this->declareAndLoadParameter("side_a2b.qos_depths", side_a2b_qos_depths_,
                                "Side A to side B minimum/fallback QoS KeepLast depth per topic/type");
  side_a2b_config.qos_depths = side_a2b_qos_depths_;
  DirectionParameters side_b2a_config{
      declare_string_array_parameter("side_b2a.topics", std::vector<std::string>{},
                                     "Side B to side A topics; explicit topics in static mode or __auto__ for auto-discovery",
                                     true),
      declare_string_array_parameter("side_b2a.topic_types", std::vector<std::string>{},
                                     "Side B to side A message types; one type per topic or auto-discovery type filters",
                                     false),
      declare_string_array_parameter("side_b2a.transports", std::vector<std::string>{},
                                     "Side B to side A transport selection per topic/type: udp or shm", false),
      side_b2a_qos_depths_};
  this->declareAndLoadParameter("side_b2a.qos_depths", side_b2a_qos_depths_,
                                "Side B to side A minimum/fallback QoS KeepLast depth per topic/type");
  side_b2a_config.qos_depths = side_b2a_qos_depths_;

  side_a2b_topics_ = side_a2b_config.topics;
  side_a2b_topic_types_ = side_a2b_config.topic_types;
  side_a2b_transports_ = side_a2b_config.transports;
  side_a2b_qos_depths_ = side_a2b_config.qos_depths;
  side_b2a_topics_ = side_b2a_config.topics;
  side_b2a_topic_types_ = side_b2a_config.topic_types;
  side_b2a_transports_ = side_b2a_config.transports;
  side_b2a_qos_depths_ = side_b2a_config.qos_depths;

  auto normalize_auto_discovery_topics = [](std::vector<std::string>& topics) {
    if (topics.size() != 1U) {
      return;
    }
    std::string value = topics.front();
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value.empty() || value == "__auto__" || value == "__auto_discovery__") {
      topics.clear();
    }
  };
  normalize_auto_discovery_topics(side_a2b_topics_);
  normalize_auto_discovery_topics(side_b2a_topics_);

  if (auto_discovery_wait_ms_ < 0) {
    throw std::runtime_error("Parameter 'auto_discovery_wait_ms' must be greater than or equal to zero.");
  }
  if (auto_discovery_poll_ms_ <= 0) {
    throw std::runtime_error("Parameter 'auto_discovery_poll_ms' must be greater than zero.");
  }

  auto canonical_transport = [](std::string transport) -> std::string {
    std::transform(transport.begin(), transport.end(), transport.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (transport == "udp") {
      return "udp";
    }
    if (transport == "shm" || transport == "shared_memory" || transport == "shared-memory") {
      return "shm";
    }
    throw std::runtime_error("Unsupported transport '" + transport + "'. Supported: udp, shm.");
  };

  auto validate_static_direction = [&canonical_transport](
                                       const std::string& direction_name, const std::vector<std::string>& topics,
                                       const std::vector<std::string>& types, const std::vector<std::string>& transports,
                                       const std::vector<int64_t>& qos_depths) {
    if (topics.size() != types.size()) {
      throw std::runtime_error("Parameters '" + direction_name + ".topics' and '" + direction_name +
                               ".topic_types' must have identical lengths.");
    }
    if (!transports.empty() && transports.size() != 1U && transports.size() != topics.size()) {
      throw std::runtime_error("Parameter '" + direction_name + ".transports' must be empty, contain one value, or match '" +
                               direction_name + ".topics'.");
    }
    if (!qos_depths.empty() && qos_depths.size() != 1U && qos_depths.size() != topics.size()) {
      throw std::runtime_error("Parameter '" + direction_name + ".qos_depths' must be empty, contain one value, or match '" +
                               direction_name + ".topics'.");
    }

    for (std::size_t idx = 0; idx < topics.size(); ++idx) {
      if (topics[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topics' contains an empty entry at index " +
                                 std::to_string(idx) + ".");
      }
      if (types[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topic_types' contains an empty entry at index " +
                                 std::to_string(idx) + ".");
      }
      const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
      if (qos_depth <= 0) {
        throw std::runtime_error("QoS depth must be greater than zero.");
      }
      if (!transports.empty()) {
        try {
          (void)canonical_transport(transports.size() == 1U ? transports.front() : transports[idx]);
        } catch (const std::runtime_error& ex) {
          throw std::runtime_error("Parameter '" + direction_name + ".transports' contains unsupported value at index " +
                                   std::to_string(idx) + ": " + ex.what());
        }
      }
    }
  };

  auto validate_auto_direction = [&canonical_transport](const std::string& direction_name, const std::vector<std::string>& types,
                                                        const std::vector<std::string>& transports,
                                                        const std::vector<int64_t>& qos_depths) {
    if (types.empty()) {
      throw std::runtime_error("Auto-discovery for '" + direction_name + "' requires at least one entry in '" + direction_name +
                               ".topic_types'.");
    }
    if (!transports.empty() && transports.size() != 1U && transports.size() != types.size()) {
      throw std::runtime_error("Parameter '" + direction_name + ".transports' must be empty, contain one value, or match '" +
                               direction_name + ".topic_types' in auto-discovery mode.");
    }
    if (!qos_depths.empty() && qos_depths.size() != 1U && qos_depths.size() != types.size()) {
      throw std::runtime_error("Parameter '" + direction_name + ".qos_depths' must be empty, contain one value, or match '" +
                               direction_name + ".topic_types' in auto-discovery mode.");
    }

    std::unordered_set<std::string> seen_types;
    for (std::size_t idx = 0; idx < types.size(); ++idx) {
      if (types[idx].empty()) {
        throw std::runtime_error("Parameter '" + direction_name + ".topic_types' contains an empty entry at index " +
                                 std::to_string(idx) + ".");
      }
      if (!seen_types.insert(types[idx]).second) {
        throw std::runtime_error("Parameter '" + direction_name + ".topic_types' contains duplicate type '" + types[idx] +
                                 "' in auto-discovery mode. Configure each type only once.");
      }
      const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
      if (qos_depth <= 0) {
        throw std::runtime_error("QoS depth must be greater than zero.");
      }
      if (!transports.empty()) {
        try {
          (void)canonical_transport(transports.size() == 1U ? transports.front() : transports[idx]);
        } catch (const std::runtime_error& ex) {
          throw std::runtime_error("Parameter '" + direction_name + ".transports' contains unsupported value at index " +
                                   std::to_string(idx) + ": " + ex.what());
        }
      }
    }
  };

  side_a2b_auto_discovery_ = side_a2b_topics_.empty() && !side_a2b_topic_types_.empty();
  side_b2a_auto_discovery_ = side_b2a_topics_.empty() && !side_b2a_topic_types_.empty();
  auto_discovery_enabled_ = side_a2b_auto_discovery_ || side_b2a_auto_discovery_;

  if (side_a2b_auto_discovery_) {
    validate_auto_direction("side_a2b", side_a2b_topic_types_, side_a2b_transports_, side_a2b_qos_depths_);
  } else {
    validate_static_direction("side_a2b", side_a2b_topics_, side_a2b_topic_types_, side_a2b_transports_, side_a2b_qos_depths_);
  }

  if (side_b2a_auto_discovery_) {
    validate_auto_direction("side_b2a", side_b2a_topic_types_, side_b2a_transports_, side_b2a_qos_depths_);
  } else {
    validate_static_direction("side_b2a", side_b2a_topics_, side_b2a_topic_types_, side_b2a_transports_, side_b2a_qos_depths_);
  }

  const auto static_routes = side_a2b_topics_.size() + side_b2a_topics_.size();
  const auto auto_type_rules = (side_a2b_auto_discovery_ ? side_a2b_topic_types_.size() : 0U) +
                               (side_b2a_auto_discovery_ ? side_b2a_topic_types_.size() : 0U);
  if (static_routes + auto_type_rules == 0U) {
    throw std::runtime_error("At least one route must be configured in 'side_a2b' or 'side_b2a'.");
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
  if (max_udp_payload_bytes_ <= static_cast<int>(sizeof(PacketHeader)) ||
      max_udp_payload_bytes_ > static_cast<int>(kMaxUdpDatagramBytes)) {
    throw std::runtime_error("Parameter 'max_udp_payload_bytes' must be in range (" + std::to_string(sizeof(PacketHeader)) +
                             ", " + std::to_string(kMaxUdpDatagramBytes) + "].");
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

  RCLCPP_INFO(this->get_logger(),
              "Bridge config: side=%s remote_host=%s tx_port=%d rx_port=%d max_udp_payload_bytes=%d max_shm_message_bytes=%d "
              "shm_poll_interval_us=%d reassembly_timeout_ms=%d side_a2b_static=%zu side_b2a_static=%zu side_a2b_auto=%s "
              "side_b2a_auto=%s",
              bridge_side_.c_str(), remote_host_.c_str(), tx_port_, rx_port_, max_udp_payload_bytes_, max_shm_message_bytes_,
              shm_poll_interval_us_, reassembly_timeout_ms_, side_a2b_topics_.size(), side_b2a_topics_.size(),
              side_a2b_auto_discovery_ ? "true" : "false", side_b2a_auto_discovery_ ? "true" : "false");
}

MiddlewareBridge::BridgeQosProfile MiddlewareBridge::defaultQosForTopic(const std::string& topic_name,
                                                                        const std::size_t fallback_depth) const {
  BridgeQosProfile qos;
  qos.depth = std::max<std::size_t>(1U, fallback_depth);
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = isTfStaticTopic(topic_name) ? RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL : RMW_QOS_POLICY_DURABILITY_VOLATILE;
  return qos;
}

MiddlewareBridge::BridgeQosProfile MiddlewareBridge::resolveSourceQos(const std::string& topic_name,
                                                                      const std::size_t fallback_depth) const {
  auto resolved = defaultQosForTopic(topic_name, fallback_depth);
  const auto endpoints = this->get_publishers_info_by_topic(topic_name);
  if (endpoints.empty()) {
    return resolved;
  }

  bool saw_best_effort = false;
  bool saw_reliable = false;
  bool saw_transient_local = false;
  bool saw_volatile = false;
  bool saw_keep_all = false;
  bool saw_keep_last = false;
  std::size_t max_depth = resolved.depth;

  for (const auto& info : endpoints) {
    rclcpp::QoS endpoint_qos(0);
    endpoint_qos = info.qos_profile();
    const auto& rmw_qos = endpoint_qos.get_rmw_qos_profile();

    if (rmw_qos.depth > 0U) {
      max_depth = std::max<std::size_t>(max_depth, static_cast<std::size_t>(rmw_qos.depth));
    }

    switch (rmw_qos.reliability) {
      case RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT:
        saw_best_effort = true;
        break;
      case RMW_QOS_POLICY_RELIABILITY_RELIABLE:
        saw_reliable = true;
        break;
      default:
        break;
    }

    switch (rmw_qos.durability) {
      case RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL:
        saw_transient_local = true;
        break;
      case RMW_QOS_POLICY_DURABILITY_VOLATILE:
        saw_volatile = true;
        break;
      default:
        break;
    }

    switch (rmw_qos.history) {
      case RMW_QOS_POLICY_HISTORY_KEEP_ALL:
        saw_keep_all = true;
        break;
      case RMW_QOS_POLICY_HISTORY_KEEP_LAST:
        saw_keep_last = true;
        break;
      default:
        break;
    }
  }

  resolved.depth = max_depth;
  if (saw_best_effort) {
    resolved.reliability = RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT;
  } else if (saw_reliable) {
    resolved.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  }

  if (isTfTopic(topic_name)) {
    resolved.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  } else if (isTfStaticTopic(topic_name)) {
    resolved.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  } else if (saw_transient_local && !saw_volatile) {
    resolved.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  } else if (saw_volatile) {
    resolved.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  }

  if (saw_keep_all) {
    resolved.history = RMW_QOS_POLICY_HISTORY_KEEP_ALL;
  } else if (saw_keep_last) {
    resolved.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  }

  if (saw_best_effort && saw_reliable) {
    RCLCPP_WARN(this->get_logger(),
                "Topic '%s' has mixed publisher reliability. Using best_effort so the bridge subscription remains compatible.",
                topic_name.c_str());
  }
  if (saw_transient_local && saw_volatile) {
    RCLCPP_WARN(this->get_logger(), "Topic '%s' has mixed publisher durability. Using durability policy %d.", topic_name.c_str(),
                static_cast<int>(resolved.durability));
  }

  return resolved;
}

bool MiddlewareBridge::qosProfilesEqual(const BridgeQosProfile& lhs, const BridgeQosProfile& rhs) {
  return lhs.depth == rhs.depth && lhs.history == rhs.history && lhs.reliability == rhs.reliability &&
         lhs.durability == rhs.durability;
}

rclcpp::QoS MiddlewareBridge::makeRclcppQos(const BridgeQosProfile& qos) const {
  rclcpp::QoS rclcpp_qos(rclcpp::KeepLast(qos.depth));
  auto& rmw_qos = rclcpp_qos.get_rmw_qos_profile();
  rmw_qos.depth = qos.depth;
  rmw_qos.history = qos.history;
  rmw_qos.reliability = qos.reliability;
  rmw_qos.durability = qos.durability;
  return rclcpp_qos;
}

void MiddlewareBridge::createChannelEndpoints(BridgeChannel& channel, const std::size_t channel_index) {
  if (!channel.publish_topic.empty()) {
    auto publisher_qos = channel.qos;
    if (isTfStaticTopic(channel.publish_topic)) {
      publisher_qos.depth = 1U;
      publisher_qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
      publisher_qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
      publisher_qos.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
    } else if (isTfTopic(channel.publish_topic)) {
      publisher_qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
    }
    channel.publisher = this->create_generic_publisher(channel.publish_topic, channel.topic_type, makeRclcppQos(publisher_qos));
  }
  if (!channel.subscribe_topic.empty()) {
    channel.subscriber =
        this->create_generic_subscription(channel.subscribe_topic, channel.topic_type, makeRclcppQos(channel.qos),
                                          [this, channel_index](std::shared_ptr<rclcpp::SerializedMessage> msg) {
                                            if (msg != nullptr) {
                                              this->forwardSerializedMessage(channel_index, *msg);
                                            }
                                          });
  }
}

void MiddlewareBridge::updateChannelQos(const std::size_t channel_index, const BridgeQosProfile& qos, const char* reason) {
  std::lock_guard<std::mutex> lock(channels_mutex_);
  if (channel_index >= channels_.size() || qosProfilesEqual(channels_[channel_index].qos, qos)) {
    return;
  }

  auto& channel = channels_[channel_index];
  channel.publisher.reset();
  channel.subscriber.reset();
  channel.qos = qos;
  createChannelEndpoints(channel, channel_index);

  RCLCPP_INFO(this->get_logger(), "Updated QoS for rule %zu from %s: reliability=%d durability=%d history=%d depth=%zu",
              channel_index, reason, static_cast<int>(channel.qos.reliability), static_cast<int>(channel.qos.durability),
              static_cast<int>(channel.qos.history), channel.qos.depth);
}

std::size_t MiddlewareBridge::addChannelIfMissing(const bool is_side_a_to_b,
                                                  const std::string& topic_name,
                                                  const std::string& topic_type,
                                                  const std::string& transport,
                                                  const BridgeQosProfile& qos,
                                                  const bool from_auto_discovery,
                                                  bool* added) {
  auto canonical_transport = [](std::string value) -> std::string {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value == "udp") {
      return "udp";
    }
    if (value == "shm" || value == "shared_memory" || value == "shared-memory") {
      return "shm";
    }
    throw std::runtime_error("Unsupported transport '" + value + "'. Supported: udp, shm.");
  };

  const std::string canonical = canonical_transport(transport);
  const bool is_side_a = bridge_side_ == "a";
  const std::string subscribe_topic = is_side_a_to_b ? (is_side_a ? topic_name : "") : (is_side_a ? "" : topic_name);
  const std::string publish_topic = is_side_a_to_b ? (is_side_a ? "" : topic_name) : (is_side_a ? topic_name : "");
  const std::string channel_key = std::string(is_side_a_to_b ? "a2b|" : "b2a|") + topic_name + "|" + topic_type;

  std::lock_guard<std::mutex> lock(channels_mutex_);
  if (channel_keys_.count(channel_key) > 0U) {
    if (added != nullptr) {
      *added = false;
    }
    for (std::size_t idx = 0; idx < channels_.size(); ++idx) {
      auto& channel = channels_[idx];
      if (channel.topic_type == topic_type && channel.subscribe_topic == subscribe_topic &&
          channel.publish_topic == publish_topic) {
        if (!qosProfilesEqual(channel.qos, qos)) {
          channel.publisher.reset();
          channel.subscriber.reset();
          channel.qos = qos;
          createChannelEndpoints(channel, idx);
          RCLCPP_INFO(this->get_logger(), "Updated QoS for rule %zu: reliability=%d durability=%d history=%d depth=%zu", idx,
                      static_cast<int>(channel.qos.reliability), static_cast<int>(channel.qos.durability),
                      static_cast<int>(channel.qos.history), channel.qos.depth);
        }
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
  channel.qos = qos;
  channel.from_auto_discovery = from_auto_discovery;
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
  createChannelEndpoints(channel, channel_index);

  channels_.push_back(std::move(channel));
  channel_keys_.insert(channel_key);

  if (channels_.back().transport == TransportType::Shm && receiver_running_.load()) {
    const auto sanitizeName = [](const std::string& raw) -> std::string {
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

  RCLCPP_INFO(this->get_logger(), "Rule %zu (%s, transport=%s%s): subscribe '%s' -> bridge -> publish '%s' (%s, depth=%zu)",
              channel_index, direction.c_str(), channels_.back().transport_name.c_str(), from_auto_discovery ? ", auto" : "",
              channels_.back().subscribe_topic.c_str(), channels_.back().publish_topic.c_str(),
              channels_.back().topic_type.c_str(), channels_.back().qos.depth);
  RCLCPP_INFO(this->get_logger(), "Rule %zu QoS: reliability=%d durability=%d history=%d depth=%zu", channel_index,
              static_cast<int>(channels_.back().qos.reliability), static_cast<int>(channels_.back().qos.durability),
              static_cast<int>(channels_.back().qos.history), channels_.back().qos.depth);
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
  channels_.reserve(side_a2b_topics_.size() + side_b2a_topics_.size());

  auto qosDepthForRule = [](const std::vector<int64_t>& qos_depths, std::size_t idx) -> std::size_t {
    const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
    return static_cast<std::size_t>(qos_depth);
  };
  auto transportForRule = [](const std::vector<std::string>& transports, std::size_t idx) -> std::string {
    if (transports.empty()) {
      return "udp";
    }
    return transports.size() == 1U ? transports.front() : transports[idx];
  };

  for (std::size_t idx = 0; idx < side_a2b_topics_.size(); ++idx) {
    const auto fallback_depth = qosDepthForRule(side_a2b_qos_depths_, idx);
    const auto qos = bridge_side_ == "a" ? resolveSourceQos(side_a2b_topics_[idx], fallback_depth)
                                         : defaultQosForTopic(side_a2b_topics_[idx], fallback_depth);
    (void)addChannelIfMissing(true, side_a2b_topics_[idx], side_a2b_topic_types_[idx],
                              transportForRule(side_a2b_transports_, idx), qos, false);
  }
  for (std::size_t idx = 0; idx < side_b2a_topics_.size(); ++idx) {
    const auto fallback_depth = qosDepthForRule(side_b2a_qos_depths_, idx);
    const auto qos = bridge_side_ == "b" ? resolveSourceQos(side_b2a_topics_[idx], fallback_depth)
                                         : defaultQosForTopic(side_b2a_topics_[idx], fallback_depth);
    (void)addChannelIfMissing(false, side_b2a_topics_[idx], side_b2a_topic_types_[idx],
                              transportForRule(side_b2a_transports_, idx), qos, false);
  }

  auto mayUseUdp = [](const std::vector<std::string>& transports) -> bool {
    if (transports.empty()) {
      return true;
    }
    for (const auto& raw : transports) {
      std::string value = raw;
      std::transform(value.begin(), value.end(), value.begin(),
                     [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      if (value == "udp") {
        return true;
      }
    }
    return false;
  };
  auto mayUseShm = [](const std::vector<std::string>& transports) -> bool {
    for (const auto& raw : transports) {
      std::string value = raw;
      std::transform(value.begin(), value.end(), value.begin(),
                     [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      if (value == "shm" || value == "shared_memory" || value == "shared-memory") {
        return true;
      }
    }
    return false;
  };

  if (side_a2b_auto_discovery_) {
    use_udp_transport_ = use_udp_transport_ || mayUseUdp(side_a2b_transports_);
    use_shm_transport_ = use_shm_transport_ || mayUseShm(side_a2b_transports_);
  }
  if (side_b2a_auto_discovery_) {
    use_udp_transport_ = use_udp_transport_ || mayUseUdp(side_b2a_transports_);
    use_shm_transport_ = use_shm_transport_ || mayUseShm(side_b2a_transports_);
  }

  if (auto_discovery_enabled_ || !channels_.empty()) {
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

  struct sockaddr_in rx_address{};
  rx_address.sin_family = AF_INET;
  rx_address.sin_port = htons(static_cast<uint16_t>(rx_port_));
  rx_address.sin_addr.s_addr = htonl(INADDR_ANY);
  if (::bind(rx_socket_fd_, reinterpret_cast<struct sockaddr*>(&rx_address), sizeof(rx_address)) != 0) {
    throw std::runtime_error("Failed to bind RX socket on port " + std::to_string(rx_port_) + ": " + std::strerror(errno));
  }
}

void MiddlewareBridge::setupSharedMemoryChannel(BridgeChannel& channel, const std::string& ns, const std::size_t channel_index) {
  if (channel.shm_fd >= 0 && channel.shm_mapping != nullptr) {
    return;
  }

  const std::size_t mapping_size = sizeof(ShmChannelHeader) + static_cast<std::size_t>(max_shm_message_bytes_);
  const std::string shm_name = "/mb_" + ns + "_ch_" + std::to_string(channel_index);
  struct statvfs shm_stats{};
  if (::statvfs("/dev/shm", &shm_stats) == 0) {
    const std::uint64_t available_bytes =
        static_cast<std::uint64_t>(shm_stats.f_bavail) * static_cast<std::uint64_t>(shm_stats.f_frsize);
    if (available_bytes < static_cast<std::uint64_t>(mapping_size)) {
      RCLCPP_WARN(this->get_logger(),
                  "Shared-memory channel '%s' requests %zu bytes, but /dev/shm has only %llu bytes available. "
                  "Consider reducing max_shm_message_bytes, using fewer SHM channels, or increasing container --shm-size.",
                  shm_name.c_str(), mapping_size, static_cast<unsigned long long>(available_bytes));
    }
  }

  const int fd = ::shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    throw std::runtime_error("Failed to open shared memory '" + shm_name + "': " + std::strerror(errno));
  }

  if (::ftruncate(fd, static_cast<off_t>(mapping_size)) != 0) {
    const std::string error = "Failed to resize shared memory '" + shm_name + "': " + std::strerror(errno);
    ::close(fd);
    throw std::runtime_error(error);
  }

  void* mapping = ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mapping == MAP_FAILED) {
    const std::string error = "Failed to map shared memory '" + shm_name + "': " + std::strerror(errno);
    ::close(fd);
    throw std::runtime_error(error);
  }

  auto* header = reinterpret_cast<ShmChannelHeader*>(mapping);
  auto* payload = reinterpret_cast<std::uint8_t*>(mapping) + sizeof(ShmChannelHeader);
  const auto expected_capacity = static_cast<std::uint32_t>(max_shm_message_bytes_);
  const auto existing_magic = header->magic.load(std::memory_order_acquire);
  const auto existing_capacity = header->capacity_bytes.load(std::memory_order_acquire);
  if (existing_magic != kShmMagic || existing_capacity != expected_capacity) {
    header->capacity_bytes.store(expected_capacity, std::memory_order_relaxed);
    header->sequence.store(0U, std::memory_order_relaxed);
    header->payload_size.store(0U, std::memory_order_relaxed);
    header->reserved.store(0U, std::memory_order_relaxed);
    // Avoid touching the full SHM payload region during setup. In constrained
    // /dev/shm environments this can trigger SIGBUS even before first payload write.
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

  RCLCPP_INFO(this->get_logger(), "Rule %zu uses shared memory '%s' (capacity=%d bytes)", channel_index, shm_name.c_str(),
              max_shm_message_bytes_);
}

void MiddlewareBridge::setupSharedMemoryChannels() {
  const auto sanitizeName = [](const std::string& raw) -> std::string {
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
    auto& channel = channels_[channel_index];
    if (channel.transport != TransportType::Shm) {
      continue;
    }
    setupSharedMemoryChannel(channel, ns, channel_index);
  }
}

void MiddlewareBridge::refreshLocalSourceQos() {
  struct SourceChannel {
    std::size_t channel_index;
    std::string topic_name;
    std::size_t fallback_depth;
  };

  std::vector<SourceChannel> source_channels;
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    source_channels.reserve(channels_.size());
    for (std::size_t channel_index = 0; channel_index < channels_.size(); ++channel_index) {
      const auto& channel = channels_[channel_index];
      if (!channel.subscribe_topic.empty()) {
        source_channels.push_back(SourceChannel{channel_index, channel.subscribe_topic, channel.qos.depth});
      }
    }
  }

  for (const auto& channel : source_channels) {
    const auto qos = resolveSourceQos(channel.topic_name, channel.fallback_depth);
    updateChannelQos(channel.channel_index, qos, "source publisher graph");
  }
}

void MiddlewareBridge::announceStaticSourceChannels() {
  struct StaticSourceChannel {
    std::uint16_t channel_id;
    bool is_side_a_to_b;
    std::string topic_name;
    std::string topic_type;
    std::string transport;
    BridgeQosProfile qos;
  };

  std::vector<StaticSourceChannel> static_source_channels;
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    static_source_channels.reserve(channels_.size());
    for (std::size_t channel_index = 0; channel_index < channels_.size(); ++channel_index) {
      const auto& channel = channels_[channel_index];
      if (channel.from_auto_discovery || channel.subscribe_topic.empty() ||
          channel_index >= static_cast<std::size_t>(kControlChannelId)) {
        continue;
      }
      static_source_channels.push_back(StaticSourceChannel{static_cast<std::uint16_t>(channel_index), bridge_side_ == "a",
                                                           channel.subscribe_topic, channel.topic_type, channel.transport_name,
                                                           channel.qos});
    }
  }

  for (const auto& channel : static_source_channels) {
    announceAutoDiscoveredChannel(channel.channel_id, channel.is_side_a_to_b, channel.topic_name, channel.topic_type,
                                  channel.transport, channel.qos);
  }
}

void MiddlewareBridge::runAutoDiscoveryScan() {
  if (!auto_discovery_enabled_) {
    return;
  }

  const auto topic_graph = this->get_topic_names_and_types();
  std::size_t added_count = 0;

  auto qosDepthForRule = [](const std::vector<int64_t>& qos_depths, std::size_t idx) -> std::size_t {
    const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
    return static_cast<std::size_t>(qos_depth);
  };
  auto transportForRule = [](const std::vector<std::string>& transports, std::size_t idx) -> std::string {
    if (transports.empty()) {
      return "udp";
    }
    return transports.size() == 1U ? transports.front() : transports[idx];
  };

  auto scanDirection = [&](const bool enabled, const bool is_side_a_to_b, const std::string& direction_name,
                           const std::vector<std::string>& topic_types, const std::vector<std::string>& transports,
                           const std::vector<int64_t>& qos_depths) {
    if (!enabled) {
      return;
    }

    std::unordered_set<std::string> seen_topics_this_scan;
    for (std::size_t rule_index = 0; rule_index < topic_types.size(); ++rule_index) {
      const std::string& type_name = topic_types[rule_index];
      std::vector<std::string> matched_topics;
      for (const auto& [topic_name, topic_type_list] : topic_graph) {
        if (std::find(topic_type_list.begin(), topic_type_list.end(), type_name) != topic_type_list.end()) {
          matched_topics.push_back(topic_name);
        }
      }
      std::sort(matched_topics.begin(), matched_topics.end());

      const auto transport = transportForRule(transports, rule_index);
      const auto fallback_depth = qosDepthForRule(qos_depths, rule_index);
      for (const auto& topic_name : matched_topics) {
        if (!seen_topics_this_scan.insert(topic_name).second) {
          RCLCPP_WARN(this->get_logger(),
                      "Skipping auto-discovered topic '%s' in %s because it matched multiple configured types.",
                      topic_name.c_str(), direction_name.c_str());
          continue;
        }
        const auto qos = resolveSourceQos(topic_name, fallback_depth);
        bool added = false;
        const auto channel_index = addChannelIfMissing(is_side_a_to_b, topic_name, type_name, transport, qos, true, &added);
        announceAutoDiscoveredChannel(static_cast<std::uint16_t>(channel_index), is_side_a_to_b, topic_name, type_name, transport,
                                      qos);
        if (added) {
          added_count += 1U;
        }
      }
    }
  };

  // Auto-discovery is directional: only the source side of a direction scans
  // the local graph; the destination side learns channels through announcements.
  const bool discover_side_a2b_locally = side_a2b_auto_discovery_ && bridge_side_ == "a";
  const bool discover_side_b2a_locally = side_b2a_auto_discovery_ && bridge_side_ == "b";

  scanDirection(discover_side_a2b_locally, true, "side_a2b", side_a2b_topic_types_, side_a2b_transports_, side_a2b_qos_depths_);
  scanDirection(discover_side_b2a_locally, false, "side_b2a", side_b2a_topic_types_, side_b2a_transports_, side_b2a_qos_depths_);

  if (added_count > 0U) {
    RCLCPP_INFO(this->get_logger(), "Auto-discovery added %zu new channel(s).", added_count);
  }
}

void MiddlewareBridge::sendUdpPayload(const std::uint16_t channel_id,
                                      const std::uint8_t* payload,
                                      const std::size_t payload_size) {
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
    RCLCPP_WARN(this->get_logger(), "Dropping UDP payload because required fragment count %zu exceeds limit %u.", fragment_count,
                static_cast<unsigned int>(std::numeric_limits<std::uint16_t>::max()));
    return;
  }

  const std::uint32_t message_id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock(send_mutex_);
  for (std::size_t fragment_index = 0; fragment_index < fragment_count; ++fragment_index) {
    const std::size_t fragment_offset = fragment_index * max_fragment_payload_size;
    const std::size_t fragment_payload_size =
        fragment_offset < payload_size ? std::min(max_fragment_payload_size, payload_size - fragment_offset) : 0U;

    PacketHeader wire_header{};
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

    const ssize_t bytes_sent = ::sendto(tx_socket_fd_, packet.data(), packet.size(), 0,
                                        reinterpret_cast<struct sockaddr*>(&tx_address_), sizeof(tx_address_));
    if (bytes_sent < 0 || static_cast<std::size_t>(bytes_sent) != packet.size()) {
      RCLCPP_WARN(this->get_logger(), "Failed UDP send for channel %u: %s", channel_id, std::strerror(errno));
      return;
    }
  }
}

void MiddlewareBridge::announceAutoDiscoveredChannel(const std::uint16_t channel_id,
                                                     const bool is_side_a_to_b,
                                                     const std::string& topic_name,
                                                     const std::string& topic_type,
                                                     const std::string& transport,
                                                     const BridgeQosProfile& qos) {
  if (tx_socket_fd_ < 0) {
    return;
  }

  std::string normalized_transport = transport;
  std::transform(normalized_transport.begin(), normalized_transport.end(), normalized_transport.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (normalized_transport == "shared_memory" || normalized_transport == "shared-memory") {
    normalized_transport = "shm";
  }

  const std::string payload = "A2|" + std::to_string(channel_id) + "|" + (is_side_a_to_b ? "a2b" : "b2a") + "|" +
                              normalized_transport + "|" + std::to_string(qos.depth) + "|" +
                              std::to_string(static_cast<int>(qos.reliability)) + "|" +
                              std::to_string(static_cast<int>(qos.durability)) + "|" +
                              std::to_string(static_cast<int>(qos.history)) + "|" + topic_name + "|" + topic_type;
  sendUdpPayload(kControlChannelId, reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
}

void MiddlewareBridge::handleAutoDiscoveryAnnouncement(const std::uint8_t* payload, const std::size_t payload_size) {
  if (payload == nullptr || payload_size == 0U) {
    return;
  }

  const std::string message(reinterpret_cast<const char*>(payload), payload_size);
  std::vector<std::string> fields;
  fields.reserve(10);
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

  if (fields.size() != 10U || fields[0] != "A2") {
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

  const bool is_side_a_to_b = fields[2] == "a2b";
  if (!is_side_a_to_b && fields[2] != "b2a") {
    return;
  }

  const std::string& transport = fields[3];
  const std::string& topic_name = fields[8];
  const std::string& topic_type = fields[9];
  if (topic_name.empty() || topic_type.empty()) {
    return;
  }

  auto qos = defaultQosForTopic(topic_name, qos_depth);
  qos.depth = qos_depth;
  try {
    qos.reliability = static_cast<rmw_qos_reliability_policy_t>(std::stoi(fields[5]));
    qos.durability = static_cast<rmw_qos_durability_policy_t>(std::stoi(fields[6]));
    qos.history = static_cast<rmw_qos_history_policy_t>(std::stoi(fields[7]));
  } catch (...) {
    return;
  }

  bool added = false;
  const auto local_channel_index = addChannelIfMissing(is_side_a_to_b, topic_name, topic_type, transport, qos, true, &added);
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
  const std::size_t receive_buffer_size =
      static_cast<std::size_t>(std::max(4096, std::max(socket_buffer_bytes_, max_udp_payload_bytes_)));
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

  auto publish_serialized = [this](std::size_t channel_index, const std::uint8_t* data, std::size_t size) {
    rclcpp::GenericPublisher::SharedPtr publisher;
    {
      std::lock_guard<std::mutex> lock(channels_mutex_);
      if (channel_index >= channels_.size()) {
        return;
      }
      auto& channel = channels_[channel_index];
      if (!channel.publisher || channel.transport != TransportType::Udp) {
        return;
      }
      publisher = channel.publisher;
    }

    rclcpp::SerializedMessage serialized_message(size);
    auto& rcl_serialized = serialized_message.get_rcl_serialized_message();
    if (size > 0U) {
      std::memcpy(rcl_serialized.buffer, data, size);
    }
    rcl_serialized.buffer_length = size;
    publisher->publish(serialized_message);
  };

  struct sockaddr_in source_address{};
  socklen_t source_length = sizeof(source_address);

  while (receiver_running_.load() && rclcpp::ok()) {
    if ((++received_datagrams % 64U) == 0U) {
      clear_stale_reassemblies();
    }

    source_length = sizeof(source_address);
    const ssize_t received_bytes = ::recvfrom(rx_socket_fd_, receive_buffer.data(), receive_buffer.size(), 0,
                                              reinterpret_cast<struct sockaddr*>(&source_address), &source_length);

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

    PacketHeader wire_header{};
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

    const std::uint64_t fragment_end_offset =
        static_cast<std::uint64_t>(fragment_offset) + static_cast<std::uint64_t>(fragment_payload_size);
    if (fragment_end_offset > static_cast<std::uint64_t>(total_payload_size)) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with invalid payload bounds.");
      continue;
    }

    const auto expected_size = header_size + static_cast<std::size_t>(fragment_payload_size);
    if (static_cast<std::size_t>(received_bytes) != expected_size) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with invalid fragment size: declared=%u bytes total_received=%zd",
                  fragment_payload_size, received_bytes);
      continue;
    }

    const std::uint8_t* fragment_data = receive_buffer.data() + header_size;

    if (fragment_count == 1U) {
      if (fragment_index != 0U || fragment_offset != 0U || total_payload_size != fragment_payload_size) {
        RCLCPP_WARN(this->get_logger(), "Dropped malformed single-fragment packet.");
        continue;
      }
      publish_serialized(local_channel_index, fragment_data, total_payload_size);
      continue;
    }

    const std::uint64_t reassembly_key =
        (static_cast<std::uint64_t>(channel_index) << 32U) | static_cast<std::uint64_t>(message_id);
    auto& state = reassembly_states[reassembly_key];
    if (state.payload.size() != total_payload_size || state.fragment_received.size() != fragment_count) {
      state.payload.assign(total_payload_size, 0U);
      state.fragment_received.assign(fragment_count, 0U);
      state.fragments_received = 0U;
      state.bytes_received = 0U;
    }
    state.last_update = std::chrono::steady_clock::now();

    if (state.fragment_received[fragment_index] == 0U) {
      if (fragment_payload_size > 0U) {
        std::memcpy(state.payload.data() + fragment_offset, fragment_data, fragment_payload_size);
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
        auto& channel = channels_[channel_index];
        if (channel.transport != TransportType::Shm || !channel.publisher || channel.shm_header == nullptr ||
            channel.shm_payload == nullptr) {
          continue;
        }

        const std::uint64_t sequence_begin = channel.shm_header->sequence.load(std::memory_order_acquire);
        if ((sequence_begin & 1U) != 0U || sequence_begin == channel.shm_last_sequence) {
          continue;
        }

        const std::uint32_t payload_size = channel.shm_header->payload_size.load(std::memory_order_acquire);
        if (payload_size > static_cast<std::uint32_t>(max_shm_message_bytes_)) {
          RCLCPP_WARN(this->get_logger(),
                      "Dropping SHM message on channel %zu because payload_size=%u exceeds max_shm_message_bytes=%d.",
                      channel_index, payload_size, max_shm_message_bytes_);
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

    for (auto& pending : pending_publishes) {
      rclcpp::SerializedMessage serialized_message(pending.payload.size());
      auto& rcl_serialized = serialized_message.get_rcl_serialized_message();
      if (!pending.payload.empty()) {
        std::memcpy(rcl_serialized.buffer, pending.payload.data(), pending.payload.size());
      }
      rcl_serialized.buffer_length = pending.payload.size();
      pending.publisher->publish(serialized_message);
    }

    std::this_thread::sleep_for(std::chrono::microseconds(shm_poll_interval_us_));
  }
}

bool MiddlewareBridge::aggregateTfStaticMessage(std::size_t channel_index,
                                                rclcpp::SerializedMessage& message,
                                                rclcpp::SerializedMessage& aggregated_message) {
  tf2_msgs::msg::TFMessage incoming;
  rclcpp::Serialization<tf2_msgs::msg::TFMessage> serializer;
  try {
    serializer.deserialize_message(&message, &incoming);
  } catch (const std::exception& ex) {
    RCLCPP_WARN(this->get_logger(), "Dropping /tf_static message that could not be deserialized: %s", ex.what());
    return false;
  }

  tf2_msgs::msg::TFMessage aggregate;
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    if (channel_index >= channels_.size()) {
      return false;
    }

    auto& channel = channels_[channel_index];
    if (!isTfStaticTopic(channel.subscribe_topic)) {
      return false;
    }

    for (const auto& transform : incoming.transforms) {
      const auto& child_frame_id = transform.child_frame_id;
      if (child_frame_id.empty()) {
        continue;
      }
      if (channel.tf_static_transforms.find(child_frame_id) == channel.tf_static_transforms.end()) {
        channel.tf_static_order.push_back(child_frame_id);
      }
      channel.tf_static_transforms[child_frame_id] = transform;
    }

    aggregate.transforms.reserve(channel.tf_static_order.size());
    for (const auto& child_frame_id : channel.tf_static_order) {
      const auto it = channel.tf_static_transforms.find(child_frame_id);
      if (it != channel.tf_static_transforms.end()) {
        aggregate.transforms.push_back(it->second);
      }
    }
  }

  serializer.serialize_message(&aggregate, &aggregated_message);
  return true;
}

void MiddlewareBridge::forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage& message) {
  TransportType transport = TransportType::Udp;
  ShmChannelHeader* shm_header = nullptr;
  std::uint8_t* shm_payload = nullptr;
  bool aggregate_tf_static = false;

  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    if (channel_index >= channels_.size()) {
      return;
    }
    transport = channels_[channel_index].transport;
    shm_header = channels_[channel_index].shm_header;
    shm_payload = channels_[channel_index].shm_payload;
    aggregate_tf_static = isTfStaticTopic(channels_[channel_index].subscribe_topic);
  }

  rclcpp::SerializedMessage aggregated_message;
  auto* outgoing_message = &message;
  if (aggregate_tf_static) {
    if (!aggregateTfStaticMessage(channel_index, message, aggregated_message)) {
      return;
    }
    outgoing_message = &aggregated_message;
  }

  const auto& rcl_serialized = outgoing_message->get_rcl_serialized_message();
  const std::size_t payload_size = rcl_serialized.buffer_length;
  if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    RCLCPP_WARN(this->get_logger(), "Dropping oversized serialized message (%zu bytes).", payload_size);
    return;
  }

  if (transport == TransportType::Shm) {
    if (shm_header == nullptr || shm_payload == nullptr) {
      RCLCPP_WARN(this->get_logger(), "Dropping SHM message on channel %zu because SHM channel is not initialized.",
                  channel_index);
      return;
    }
    if (payload_size > static_cast<std::size_t>(max_shm_message_bytes_)) {
      RCLCPP_WARN(this->get_logger(), "Dropping SHM message on channel %zu because size=%zu exceeds max_shm_message_bytes=%d.",
                  channel_index, payload_size, max_shm_message_bytes_);
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
  for (auto& channel : channels_) {
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

}  // namespace ros_middleware_bridge

int main(int argc, char* argv[]) {
  rclcpp::init(argc, argv);

  auto node = std::make_shared<ros_middleware_bridge::MiddlewareBridge>();
  rclcpp::executors::MultiThreadedExecutor executor(rclcpp::ExecutorOptions(), node->num_threads_);
  executor.add_node(node);
  executor.spin();

  rclcpp::shutdown();
  return 0;
}
