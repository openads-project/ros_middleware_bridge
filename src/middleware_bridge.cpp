#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

#include <middleware_bridge/middleware_bridge.hpp>

namespace middleware_bridge {

MiddlewareBridge::MiddlewareBridge() : Node("middleware_bridge") {
  declareAndLoadParameters();
  setupBridgeChannels();
  setupSockets();
  receiver_running_.store(true);
  receiver_thread_ = std::thread(&MiddlewareBridge::receiverLoop, this);
}

MiddlewareBridge::~MiddlewareBridge() {
  stopReceiverThread();
}

void MiddlewareBridge::declareAndLoadParameters() {
  num_threads_ = this->declare_parameter<int>("num_threads", 1);
  bridge_role_ = this->declare_parameter<std::string>("bridge_role", "dds");
  remote_host_ = this->declare_parameter<std::string>("remote_host", "127.0.0.1");
  tx_port_ = this->declare_parameter<int>("tx_port", 17001);
  rx_port_ = this->declare_parameter<int>("rx_port", 17002);
  socket_buffer_bytes_ = this->declare_parameter<int>("socket_buffer_bytes", 1024 * 1024);
  dds2zenoh_topics_ = this->declare_parameter<std::vector<std::string>>(
      "dds2zenoh.topics", std::vector<std::string>{"/bridge/dds2zenoh/example"});
  dds2zenoh_topic_types_ = this->declare_parameter<std::vector<std::string>>(
      "dds2zenoh.topic_types", std::vector<std::string>{"geometry_msgs/msg/PointStamped"});
  dds2zenoh_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("dds2zenoh.qos_depths", std::vector<int64_t>{10});

  zenoh2dds_topics_ = this->declare_parameter<std::vector<std::string>>("zenoh2dds.topics", std::vector<std::string>{});
  zenoh2dds_topic_types_ = this->declare_parameter<std::vector<std::string>>("zenoh2dds.topic_types", std::vector<std::string>{});
  zenoh2dds_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("zenoh2dds.qos_depths", std::vector<int64_t>{});

  std::transform(
      bridge_role_.begin(),
      bridge_role_.end(),
      bridge_role_.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  if (bridge_role_ == "fast" || bridge_role_ == "fastrtps" || bridge_role_ == "fastrtps_cpp") {
    bridge_role_ = "dds";
  }
  if (bridge_role_ != "dds" && bridge_role_ != "zenoh") {
    throw std::runtime_error("Parameter 'bridge_role' must be either 'dds' or 'zenoh'.");
  }

  auto validate_direction = [](const std::string & direction_name,
                               const std::vector<std::string> & topics,
                               const std::vector<std::string> & types,
                               const std::vector<int64_t> & qos_depths) {
    if (topics.size() != types.size()) {
      throw std::runtime_error(
          "Parameters '" + direction_name + ".topics' and '" + direction_name + ".topic_types' must have identical lengths.");
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
    }
  };
  validate_direction("dds2zenoh", dds2zenoh_topics_, dds2zenoh_topic_types_, dds2zenoh_qos_depths_);
  validate_direction("zenoh2dds", zenoh2dds_topics_, zenoh2dds_topic_types_, zenoh2dds_qos_depths_);

  const auto total_rules = dds2zenoh_topics_.size() + zenoh2dds_topics_.size();
  if (total_rules == 0U) {
    throw std::runtime_error("At least one route must be configured in 'dds2zenoh' or 'zenoh2dds'.");
  }
  if (total_rules > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    throw std::runtime_error("Too many topic rules. Maximum is 65535.");
  }

  if (tx_port_ <= 0 || tx_port_ > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("Parameter 'tx_port' must be in range [1, 65535].");
  }
  if (rx_port_ <= 0 || rx_port_ > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("Parameter 'rx_port' must be in range [1, 65535].");
  }

  RCLCPP_INFO(
      this->get_logger(),
      "Bridge config: role=%s remote_host=%s tx_port=%d rx_port=%d dds2zenoh=%zu zenoh2dds=%zu",
      bridge_role_.c_str(),
      remote_host_.c_str(),
      tx_port_,
      rx_port_,
      dds2zenoh_topics_.size(),
      zenoh2dds_topics_.size());
}

void MiddlewareBridge::setupBridgeChannels() {
  channels_.reserve(dds2zenoh_topics_.size() + zenoh2dds_topics_.size());

  auto qosDepthForRule = [](const std::vector<int64_t> & qos_depths, std::size_t idx) -> std::size_t {
    const int64_t qos_depth = qos_depths.empty() ? 10 : (qos_depths.size() == 1U ? qos_depths.front() : qos_depths[idx]);
    return static_cast<std::size_t>(qos_depth);
  };

  auto addChannel = [this](const std::string & subscribe_topic,
                           const std::string & publish_topic,
                           const std::string & topic_type,
                           std::size_t qos_depth) {
    BridgeChannel channel;
    channel.subscribe_topic = subscribe_topic;
    channel.publish_topic = publish_topic;
    channel.topic_type = topic_type;
    channel.qos_depth = qos_depth;
    const std::size_t channel_index = channels_.size();

    const bool has_subscriber = !channel.subscribe_topic.empty();
    const bool has_publisher = !channel.publish_topic.empty();
    if (!has_subscriber && !has_publisher) {
      throw std::runtime_error(
          "Invalid rule at index " + std::to_string(channel_index) + ": local subscribe and publish endpoints are both empty.");
    }

    if (has_subscriber && has_publisher && channel.subscribe_topic == channel.publish_topic) {
      RCLCPP_WARN(
          this->get_logger(),
          "Rule %zu subscribes and publishes on the same topic '%s'. This can create bridge loops.",
          channel_index,
          channel.subscribe_topic.c_str());
    }

    const auto qos = rclcpp::QoS(rclcpp::KeepLast(channel.qos_depth));
    if (has_publisher) {
      channel.publisher = this->create_generic_publisher(channel.publish_topic, channel.topic_type, qos);
    }
    if (has_subscriber) {
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

    std::string direction = "tx+rx";
    if (has_subscriber && !has_publisher) {
      direction = "tx-only";
    } else if (!has_subscriber && has_publisher) {
      direction = "rx-only";
    }

    RCLCPP_INFO(
        this->get_logger(),
        "Rule %zu (%s): subscribe '%s' -> network -> publish '%s' (%s, depth=%zu)",
        channel_index,
        direction.c_str(),
        channel.subscribe_topic.c_str(),
        channel.publish_topic.c_str(),
        channel.topic_type.c_str(),
        channel.qos_depth);

    channels_.push_back(std::move(channel));
  };

  for (std::size_t idx = 0; idx < dds2zenoh_topics_.size(); ++idx) {
    const bool is_dds_role = bridge_role_ == "dds";
    addChannel(
        is_dds_role ? dds2zenoh_topics_[idx] : "",
        is_dds_role ? "" : dds2zenoh_topics_[idx],
        dds2zenoh_topic_types_[idx],
        qosDepthForRule(dds2zenoh_qos_depths_, idx));
  }

  for (std::size_t idx = 0; idx < zenoh2dds_topics_.size(); ++idx) {
    const bool is_dds_role = bridge_role_ == "dds";
    addChannel(
        is_dds_role ? "" : zenoh2dds_topics_[idx],
        is_dds_role ? zenoh2dds_topics_[idx] : "",
        zenoh2dds_topic_types_[idx],
        qosDepthForRule(zenoh2dds_qos_depths_, idx));
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

void MiddlewareBridge::receiverLoop() {
  std::vector<std::uint8_t> receive_buffer(static_cast<std::size_t>(std::max(4096, socket_buffer_bytes_)));
  struct sockaddr_in source_address {};
  socklen_t source_length = sizeof(source_address);

  while (receiver_running_.load() && rclcpp::ok()) {
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

    if (received_bytes < static_cast<ssize_t>(sizeof(PacketHeader))) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet smaller than header (%zd bytes)", received_bytes);
      continue;
    }

    PacketHeader wire_header {};
    std::memcpy(&wire_header, receive_buffer.data(), sizeof(PacketHeader));
    const auto magic = ntohl(wire_header.magic);
    const auto version = ntohs(wire_header.version);
    const auto channel_index = ntohs(wire_header.channel_index);
    const auto payload_size = ntohl(wire_header.payload_size);

    if (magic != kPacketMagic || version != kPacketVersion) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet with unsupported magic/version.");
      continue;
    }

    if (channel_index >= channels_.size()) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet for unknown channel %u", channel_index);
      continue;
    }
    if (!channels_[channel_index].publisher) {
      continue;
    }

    const auto expected_size = sizeof(PacketHeader) + static_cast<std::size_t>(payload_size);
    if (static_cast<std::size_t>(received_bytes) != expected_size) {
      RCLCPP_WARN(
          this->get_logger(),
          "Dropped packet with invalid payload size: declared=%u bytes total_received=%zd",
          payload_size,
          received_bytes);
      continue;
    }

    rclcpp::SerializedMessage serialized_message(payload_size);
    auto & rcl_serialized = serialized_message.get_rcl_serialized_message();
    if (payload_size > 0U) {
      std::memcpy(
          rcl_serialized.buffer,
          receive_buffer.data() + sizeof(PacketHeader),
          payload_size);
    }
    rcl_serialized.buffer_length = payload_size;

    channels_[channel_index].publisher->publish(serialized_message);
  }
}

void MiddlewareBridge::forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage & message) {
  if (channel_index >= channels_.size()) {
    return;
  }

  const auto & rcl_serialized = message.get_rcl_serialized_message();
  const std::size_t payload_size = rcl_serialized.buffer_length;
  if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    RCLCPP_WARN(this->get_logger(), "Dropping oversized serialized message (%zu bytes).", payload_size);
    return;
  }

  const std::size_t packet_size = sizeof(PacketHeader) + payload_size;
  if (packet_size > kMaxUdpDatagramBytes) {
    RCLCPP_WARN(
        this->get_logger(),
        "Dropping message on channel %zu because datagram size %zu exceeds UDP limit %zu.",
        channel_index,
        packet_size,
        kMaxUdpDatagramBytes);
    return;
  }
  std::vector<std::uint8_t> packet(packet_size);

  PacketHeader wire_header {};
  wire_header.magic = htonl(kPacketMagic);
  wire_header.version = htons(kPacketVersion);
  wire_header.channel_index = htons(static_cast<std::uint16_t>(channel_index));
  wire_header.payload_size = htonl(static_cast<std::uint32_t>(payload_size));

  std::memcpy(packet.data(), &wire_header, sizeof(PacketHeader));
  if (payload_size > 0U) {
    std::memcpy(packet.data() + sizeof(PacketHeader), rcl_serialized.buffer, payload_size);
  }

  std::lock_guard<std::mutex> lock(send_mutex_);
  const ssize_t bytes_sent = ::sendto(
      tx_socket_fd_,
      packet.data(),
      packet.size(),
      0,
      reinterpret_cast<struct sockaddr *>(&tx_address_),
      sizeof(tx_address_));

  if (bytes_sent < 0) {
    RCLCPP_WARN(this->get_logger(), "sendto failed: %s", std::strerror(errno));
  } else if (static_cast<std::size_t>(bytes_sent) != packet.size()) {
    RCLCPP_WARN(this->get_logger(), "Partial UDP send detected (%zd/%zu).", bytes_sent, packet.size());
  }
}

void MiddlewareBridge::stopReceiverThread() {
  const bool was_running = receiver_running_.exchange(false);
  if (!was_running) {
    return;
  }

  if (rx_socket_fd_ >= 0) {
    ::shutdown(rx_socket_fd_, SHUT_RDWR);
  }
  if (tx_socket_fd_ >= 0) {
    ::shutdown(tx_socket_fd_, SHUT_RDWR);
  }

  if (receiver_thread_.joinable()) {
    receiver_thread_.join();
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
