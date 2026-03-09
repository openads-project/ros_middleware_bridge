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
  dds2zenoh_topics_ = this->declare_parameter<std::vector<std::string>>(
      "dds2zenoh.topics", std::vector<std::string>{"/bridge/dds2zenoh/example"});
  dds2zenoh_topic_types_ = this->declare_parameter<std::vector<std::string>>(
      "dds2zenoh.topic_types", std::vector<std::string>{"geometry_msgs/msg/PointStamped"});
  dds2zenoh_transports_ = this->declare_parameter<std::vector<std::string>>("dds2zenoh.transports", std::vector<std::string>{});
  dds2zenoh_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("dds2zenoh.qos_depths", std::vector<int64_t>{10});

  zenoh2dds_topics_ = this->declare_parameter<std::vector<std::string>>("zenoh2dds.topics", std::vector<std::string>{});
  zenoh2dds_topic_types_ = this->declare_parameter<std::vector<std::string>>("zenoh2dds.topic_types", std::vector<std::string>{});
  zenoh2dds_transports_ = this->declare_parameter<std::vector<std::string>>("zenoh2dds.transports", std::vector<std::string>{});
  zenoh2dds_qos_depths_ = this->declare_parameter<std::vector<int64_t>>("zenoh2dds.qos_depths", std::vector<int64_t>{});

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
        "zenoh, zenoh_cpp, rmw_zenoh_cpp).");
  }

  auto validate_direction = [](const std::string & direction_name,
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
        std::string transport = transports.size() == 1U ? transports.front() : transports[idx];
        std::transform(
            transport.begin(),
            transport.end(),
            transport.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (transport != "udp" && transport != "shm" && transport != "shared_memory" && transport != "shared-memory") {
          throw std::runtime_error(
              "Parameter '" + direction_name + ".transports' contains unsupported transport '" + transport +
              "' at index " + std::to_string(idx) + ". Supported: udp, shm.");
        }
      }
    }
  };
  validate_direction("dds2zenoh", dds2zenoh_topics_, dds2zenoh_topic_types_, dds2zenoh_transports_, dds2zenoh_qos_depths_);
  validate_direction("zenoh2dds", zenoh2dds_topics_, zenoh2dds_topic_types_, zenoh2dds_transports_, zenoh2dds_qos_depths_);

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
      "Bridge config: role=%s remote_host=%s tx_port=%d rx_port=%d max_udp_payload_bytes=%d max_shm_message_bytes=%d shm_poll_interval_us=%d reassembly_timeout_ms=%d dds2zenoh=%zu zenoh2dds=%zu",
      bridge_role_.c_str(),
      remote_host_.c_str(),
      tx_port_,
      rx_port_,
      max_udp_payload_bytes_,
      max_shm_message_bytes_,
      shm_poll_interval_us_,
      reassembly_timeout_ms_,
      dds2zenoh_topics_.size(),
      zenoh2dds_topics_.size());
}

void MiddlewareBridge::setupBridgeChannels() {
  use_udp_transport_ = false;
  use_shm_transport_ = false;
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

  auto parseTransport = [](std::string transport) -> std::pair<TransportType, std::string> {
    std::transform(
        transport.begin(),
        transport.end(),
        transport.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (transport == "udp") {
      return {TransportType::Udp, "udp"};
    }
    if (transport == "shm" || transport == "shared_memory" || transport == "shared-memory") {
      return {TransportType::Shm, "shm"};
    }
    throw std::runtime_error("Unsupported transport '" + transport + "'. Supported: udp, shm.");
  };

  auto addChannel = [this, &parseTransport](const std::string & subscribe_topic,
                                            const std::string & publish_topic,
                                            const std::string & topic_type,
                                            const std::string & transport,
                                            std::size_t qos_depth) {
    BridgeChannel channel;
    channel.subscribe_topic = subscribe_topic;
    channel.publish_topic = publish_topic;
    channel.topic_type = topic_type;
    const auto parsed_transport = parseTransport(transport);
    channel.transport = parsed_transport.first;
    channel.transport_name = parsed_transport.second;
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

    if (channel.transport == TransportType::Udp) {
      use_udp_transport_ = true;
    } else if (channel.transport == TransportType::Shm) {
      use_shm_transport_ = true;
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
        "Rule %zu (%s, transport=%s): subscribe '%s' -> bridge -> publish '%s' (%s, depth=%zu)",
        channel_index,
        direction.c_str(),
        channel.transport_name.c_str(),
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
        transportForRule(dds2zenoh_transports_, idx),
        qosDepthForRule(dds2zenoh_qos_depths_, idx));
  }

  for (std::size_t idx = 0; idx < zenoh2dds_topics_.size(); ++idx) {
    const bool is_dds_role = bridge_role_ == "dds";
    addChannel(
        is_dds_role ? "" : zenoh2dds_topics_[idx],
        is_dds_role ? zenoh2dds_topics_[idx] : "",
        zenoh2dds_topic_types_[idx],
        transportForRule(zenoh2dds_transports_, idx),
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
  const std::size_t mapping_size = sizeof(ShmChannelHeader) + static_cast<std::size_t>(max_shm_message_bytes_);

  for (std::size_t channel_index = 0; channel_index < channels_.size(); ++channel_index) {
    auto & channel = channels_[channel_index];
    if (channel.transport != TransportType::Shm) {
      continue;
    }

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
    rclcpp::SerializedMessage serialized_message(size);
    auto & rcl_serialized = serialized_message.get_rcl_serialized_message();
    if (size > 0U) {
      std::memcpy(rcl_serialized.buffer, data, size);
    }
    rcl_serialized.buffer_length = size;
    channels_[channel_index].publisher->publish(serialized_message);
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

    if (channel_index >= channels_.size()) {
      RCLCPP_WARN(this->get_logger(), "Dropped packet for unknown channel %u", channel_index);
      continue;
    }
    if (!channels_[channel_index].publisher || channels_[channel_index].transport != TransportType::Udp) {
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
      publish_serialized(channel_index, fragment_data, total_payload_size);
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
      publish_serialized(channel_index, state.payload.data(), state.payload.size());
      reassembly_states.erase(reassembly_key);
    }
  }
}

void MiddlewareBridge::shmReceiverLoop() {
  auto publish_serialized = [this](std::size_t channel_index, const std::uint8_t * data, std::size_t size) {
    rclcpp::SerializedMessage serialized_message(size);
    auto & rcl_serialized = serialized_message.get_rcl_serialized_message();
    if (size > 0U) {
      std::memcpy(rcl_serialized.buffer, data, size);
    }
    rcl_serialized.buffer_length = size;
    channels_[channel_index].publisher->publish(serialized_message);
  };

  while (receiver_running_.load() && rclcpp::ok()) {
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

      channel.shm_read_buffer.resize(payload_size);
      if (payload_size > 0U) {
        std::memcpy(channel.shm_read_buffer.data(), channel.shm_payload, payload_size);
      }
      std::atomic_thread_fence(std::memory_order_acquire);
      const std::uint64_t sequence_end = channel.shm_header->sequence.load(std::memory_order_acquire);
      if (sequence_begin != sequence_end || (sequence_end & 1U) != 0U) {
        continue;
      }

      if (sequence_end != channel.shm_last_sequence) {
        publish_serialized(channel_index, channel.shm_read_buffer.data(), payload_size);
        channel.shm_last_sequence = sequence_end;
      }
    }

    std::this_thread::sleep_for(std::chrono::microseconds(shm_poll_interval_us_));
  }
}

void MiddlewareBridge::forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage & message) {
  if (channel_index >= channels_.size()) {
    return;
  }
  auto & channel = channels_[channel_index];

  const auto & rcl_serialized = message.get_rcl_serialized_message();
  const std::size_t payload_size = rcl_serialized.buffer_length;
  if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    RCLCPP_WARN(this->get_logger(), "Dropping oversized serialized message (%zu bytes).", payload_size);
    return;
  }

  if (channel.transport == TransportType::Shm) {
    if (channel.shm_header == nullptr || channel.shm_payload == nullptr) {
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

    std::uint64_t sequence = channel.shm_header->sequence.load(std::memory_order_relaxed);
    if ((sequence & 1U) != 0U) {
      sequence += 1U;
    }
    channel.shm_header->sequence.store(sequence + 1U, std::memory_order_release);
    channel.shm_header->payload_size.store(static_cast<std::uint32_t>(payload_size), std::memory_order_relaxed);
    if (payload_size > 0U) {
      std::memcpy(channel.shm_payload, rcl_serialized.buffer, payload_size);
    }
    std::atomic_thread_fence(std::memory_order_release);
    channel.shm_header->sequence.store(sequence + 2U, std::memory_order_release);
    return;
  }

  if (tx_socket_fd_ < 0) {
    RCLCPP_WARN(this->get_logger(), "Dropping UDP message on channel %zu because UDP socket is not initialized.", channel_index);
    return;
  }

  const std::size_t header_size = sizeof(PacketHeader);
  const std::size_t max_fragment_payload_size = static_cast<std::size_t>(max_udp_payload_bytes_) - header_size;
  const std::size_t fragment_count =
      std::max<std::size_t>(1, (payload_size + max_fragment_payload_size - 1) / max_fragment_payload_size);
  if (fragment_count > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    RCLCPP_WARN(
        this->get_logger(),
        "Dropping message on channel %zu because required fragment count %zu exceeds limit %u.",
        channel_index,
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
    wire_header.channel_index = htons(static_cast<std::uint16_t>(channel_index));
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
      std::memcpy(packet.data() + header_size, rcl_serialized.buffer + fragment_offset, fragment_payload_size);
    }

    const ssize_t bytes_sent = ::sendto(
        tx_socket_fd_,
        packet.data(),
        packet.size(),
        0,
        reinterpret_cast<struct sockaddr *>(&tx_address_),
        sizeof(tx_address_));

    if (bytes_sent < 0) {
      RCLCPP_WARN(
          this->get_logger(),
          "sendto failed for message_id=%u fragment=%zu/%zu: %s",
          message_id,
          fragment_index + 1,
          fragment_count,
          std::strerror(errno));
      return;
    }
    if (static_cast<std::size_t>(bytes_sent) != packet.size()) {
      RCLCPP_WARN(
          this->get_logger(),
          "Partial UDP send for message_id=%u fragment=%zu/%zu (%zd/%zu).",
          message_id,
          fragment_index + 1,
          fragment_count,
          bytes_sent,
          packet.size());
      return;
    }
  }
}

void MiddlewareBridge::stopBackgroundThreads() {
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
