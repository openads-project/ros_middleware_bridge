// Copyright Institute for Automotive Engineering (ika), RWTH Aachen University
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <netinet/in.h>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <rmw/types.h>
#include <rclcpp/generic_publisher.hpp>
#include <rclcpp/generic_subscription.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialized_message.hpp>
#include <tf2_msgs/msg/tf_message.hpp>

namespace ros_middleware_bridge {

class MiddlewareBridge : public rclcpp::Node {
 public:
  /**
   * @brief Construct the bridge node, load parameters, create channels, and start receiver threads.
   */
  MiddlewareBridge();

  /**
   * @brief Disable copying because the node owns sockets, threads, and shared-memory mappings.
   */
  MiddlewareBridge(const MiddlewareBridge&) = delete;

  /**
   * @brief Disable copy assignment because the node owns sockets, threads, and shared-memory mappings.
   */
  MiddlewareBridge& operator=(const MiddlewareBridge&) = delete;

  /**
   * @brief Disable moving because background threads capture this node instance.
   */
  MiddlewareBridge(MiddlewareBridge&&) = delete;

  /**
   * @brief Disable move assignment because background threads capture this node instance.
   */
  MiddlewareBridge& operator=(MiddlewareBridge&&) = delete;

  /**
   * @brief Stop receiver threads and release shared-memory resources.
   */
  ~MiddlewareBridge() override;

  int num_threads_ = 1;

 private:
  enum class TransportType {
    Udp,
    Shm,
  };

  struct ShmChannelHeader;

  struct BridgeQosProfile {
    std::size_t depth = 10;
    rmw_qos_history_policy_t history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    rmw_qos_reliability_policy_t reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    rmw_qos_durability_policy_t durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  };

  struct BridgeChannel {
    std::string subscribe_topic;
    std::string publish_topic;
    std::string topic_type;
    TransportType transport = TransportType::Udp;
    std::string transport_name = "udp";
    BridgeQosProfile qos;
    bool from_auto_discovery = false;
    rclcpp::GenericSubscription::SharedPtr subscriber;
    rclcpp::GenericPublisher::SharedPtr publisher;
    std::unordered_map<std::string, tf2_msgs::msg::TFMessage::_transforms_type::value_type> tf_static_transforms;
    std::vector<std::string> tf_static_order;

    int shm_fd = -1;
    void* shm_mapping = nullptr;
    std::size_t shm_mapping_size = 0;
    ShmChannelHeader* shm_header = nullptr;
    std::uint8_t* shm_payload = nullptr;
    std::uint64_t shm_last_sequence = 0;
    std::vector<std::uint8_t> shm_read_buffer;
  };

  struct PacketHeader {
    std::uint32_t magic;
    std::uint16_t version;
    std::uint16_t channel_index;
    std::uint32_t message_id;
    std::uint16_t fragment_index;
    std::uint16_t fragment_count;
    std::uint32_t total_payload_size;
    std::uint32_t fragment_offset;
    std::uint32_t fragment_payload_size;
  };

  struct ShmChannelHeader {
    std::atomic<std::uint32_t> magic{0};
    std::atomic<std::uint32_t> capacity_bytes{0};
    std::atomic<std::uint64_t> sequence{0};
    std::atomic<std::uint32_t> payload_size{0};
    std::atomic<std::uint32_t> reserved{0};
  };

  /**
   * @brief Declares and loads a ROS parameter
   *
   * @param[in] name name
   * @param[in] param parameter variable to load into
   * @param[in] description description
   * @param[in] add_to_auto_reconfigurable_params enable reconfiguration of parameter
   * @param[in] is_required whether failure to load parameter will stop node
   * @param[in] read_only set parameter to read-only
   * @param[in] from_value parameter range minimum
   * @param[in] to_value parameter range maximum
   * @param[in] step_value parameter range step
   * @param[in] additional_constraints additional constraints description
   */
  template <typename T>
  void declareAndLoadParameter(const std::string& name,
                               T& param,
                               const std::string& description,
                               const bool add_to_auto_reconfigurable_params = true,
                               const bool is_required = false,
                               const bool read_only = false,
                               const std::optional<double>& from_value = std::nullopt,
                               const std::optional<double>& to_value = std::nullopt,
                               const std::optional<double>& step_value = std::nullopt,
                               const std::string& additional_constraints = "");

  /**
   * @brief Declare, load, normalize, and validate all bridge parameters.
   */
  void declareAndLoadParameters();

  /**
   * @brief Build the initial static bridge channel list from the configured side A/B routes.
   */
  void setupBridgeChannels();

  /**
   * @brief Create and configure UDP transmit and receive sockets.
   */
  void setupSockets();

  /**
   * @brief Create shared-memory backing regions for all channels using SHM transport.
   */
  void setupSharedMemoryChannels();

  /**
   * @brief Create or open one shared-memory channel.
   *
   * @param[in,out] channel Channel whose SHM handles and mappings are initialized.
   * @param[in] ns Shared-memory namespace prefix.
   * @param[in] channel_index Local bridge channel index.
   */
  void setupSharedMemoryChannel(BridgeChannel& channel, const std::string& ns, std::size_t channel_index);

  /**
   * @brief Discover local source topics matching auto-discovery type filters and add channels for them.
   */
  void runAutoDiscoveryScan();

  /**
   * @brief Refresh QoS metadata for configured local source topics from currently visible publishers.
   */
  void refreshLocalSourceQos();

  /**
   * @brief Announce static local source channels to the remote bridge over the UDP control channel.
   */
  void announceStaticSourceChannels();

  /**
   * @brief Add a bridge channel if an equivalent channel does not already exist.
   *
   * @param[in] is_side_a_to_b True for side A to side B direction, false for side B to side A.
   * @param[in] topic_name ROS topic name to bridge.
   * @param[in] topic_type ROS message type of the topic.
   * @param[in] transport Transport name, either udp or shm.
   * @param[in] qos QoS profile to use for the output publisher.
   * @param[in] from_auto_discovery Whether the channel was created by auto-discovery.
   * @param[out] added Optional flag set to true when a new channel was added.
   *
   * @return Local channel index.
   */
  std::size_t addChannelIfMissing(bool is_side_a_to_b,
                                  const std::string& topic_name,
                                  const std::string& topic_type,
                                  const std::string& transport,
                                  const BridgeQosProfile& qos,
                                  bool from_auto_discovery,
                                  bool* added = nullptr);

  /**
   * @brief Create a fallback QoS profile for a topic.
   *
   * @param[in] topic_name Topic name used for special handling of TF topics.
   * @param[in] fallback_depth Minimum KeepLast queue depth.
   *
   * @return QoS profile for bridge output publishers.
   */
  static BridgeQosProfile defaultQosForTopic(const std::string& topic_name, std::size_t fallback_depth);

  /**
   * @brief Resolve QoS from discovered source publishers, falling back to a topic default.
   *
   * @param[in] topic_name Source topic to inspect.
   * @param[in] fallback_depth Minimum KeepLast queue depth.
   *
   * @return QoS profile derived from source publishers.
   */
  BridgeQosProfile resolveSourceQos(const std::string& topic_name, std::size_t fallback_depth) const;

  /**
   * @brief Compare two bridge QoS profiles for exact policy equality.
   */
  static bool qosProfilesEqual(const BridgeQosProfile& lhs, const BridgeQosProfile& rhs);

  /**
   * @brief Convert a bridge QoS profile to an rclcpp QoS object.
   */
  static rclcpp::QoS makeRclcppQos(const BridgeQosProfile& qos);

  /**
   * @brief Create the ROS subscription and publisher endpoints for a channel.
   *
   * @param[in,out] channel Channel to initialize.
   * @param[in] channel_index Local bridge channel index.
   */
  void createChannelEndpoints(BridgeChannel& channel, std::size_t channel_index);

  /**
   * @brief Recreate a channel's ROS endpoints after its QoS profile changed.
   *
   * @param[in] channel_index Local bridge channel index.
   * @param[in] qos New QoS profile.
   * @param[in] reason Human-readable reason for logging.
   */
  void updateChannelQos(std::size_t channel_index, const BridgeQosProfile& qos, const char* reason);

  /**
   * @brief Send serialized payload bytes over UDP, fragmenting when required.
   *
   * @param[in] channel_id Remote channel identifier.
   * @param[in] payload Serialized message payload.
   * @param[in] payload_size Payload size in bytes.
   */
  void sendUdpPayload(std::uint16_t channel_id, const std::uint8_t* payload, std::size_t payload_size);

  /**
   * @brief Announce one auto-discovered source channel to the remote bridge.
   *
   * @param[in] channel_id Local channel identifier to announce.
   * @param[in] is_side_a_to_b True for side A to side B direction, false for side B to side A.
   * @param[in] topic_name ROS topic name.
   * @param[in] topic_type ROS message type.
   * @param[in] transport Transport name, either udp or shm.
   * @param[in] qos QoS profile for the remote publisher.
   */
  void announceAutoDiscoveredChannel(std::uint16_t channel_id,
                                     bool is_side_a_to_b,
                                     const std::string& topic_name,
                                     const std::string& topic_type,
                                     const std::string& transport,
                                     const BridgeQosProfile& qos);

  /**
   * @brief Decode a UDP control announcement and create the corresponding receive channel.
   *
   * @param[in] payload Control payload bytes.
   * @param[in] payload_size Payload size in bytes.
   */
  void handleAutoDiscoveryAnnouncement(const std::uint8_t* payload, std::size_t payload_size);

  /**
   * @brief Aggregate forwarded /tf_static transforms into a complete latched sample.
   *
   * @param[in] channel_index Local channel index.
   * @param[in] message Incoming serialized TFMessage.
   * @param[out] aggregated_message Serialized aggregate TFMessage to publish.
   *
   * @return True when aggregation succeeded and @p aggregated_message is valid.
   */
  bool aggregateTfStaticMessage(std::size_t channel_index,
                                rclcpp::SerializedMessage& message,
                                rclcpp::SerializedMessage& aggregated_message);

  /**
   * @brief Receive UDP data and control packets until the node shuts down.
   */
  void receiverLoop();

  /**
   * @brief Poll shared-memory channels for new samples until the node shuts down.
   */
  void shmReceiverLoop();

  /**
   * @brief Forward one serialized ROS message over the configured channel transport.
   *
   * @param[in] channel_index Local channel index.
   * @param[in] message Serialized ROS message to forward.
   */
  void forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage& message);

  /**
   * @brief Request receiver thread shutdown and join background threads.
   */
  void stopBackgroundThreads();

  /**
   * @brief Unmap, close, and unlink shared-memory channel resources.
   */
  void closeSharedMemoryChannels();

  std::string remote_host_ = "127.0.0.1";
  std::string shm_namespace_ = "ros_middleware_bridge";
  std::string bridge_side_ = "a";
  int tx_port_ = 17001;
  int rx_port_ = 17002;
  int socket_buffer_bytes_ = 1024 * 1024;
  int max_udp_payload_bytes_ = 60000;
  int max_shm_message_bytes_ = 8 * 1024 * 1024;
  int shm_poll_interval_us_ = 1000;
  int reassembly_timeout_ms_ = 1000;
  int auto_discovery_wait_ms_ = 0;
  int auto_discovery_poll_ms_ = 1000;
  std::vector<std::string> side_a2b_topics_;
  std::vector<std::string> side_a2b_topic_types_;
  std::vector<std::string> side_a2b_transports_;
  std::vector<int64_t> side_a2b_qos_depths_;
  std::vector<std::string> side_b2a_topics_;
  std::vector<std::string> side_b2a_topic_types_;
  std::vector<std::string> side_b2a_transports_;
  std::vector<int64_t> side_b2a_qos_depths_;
  bool side_a2b_auto_discovery_ = false;
  bool side_b2a_auto_discovery_ = false;
  bool auto_discovery_enabled_ = false;
  std::unordered_set<std::string> channel_keys_;
  std::unordered_map<std::uint16_t, std::size_t> remote_channel_to_local_index_;
  std::vector<BridgeChannel> channels_;
  std::mutex channels_mutex_;
  bool use_udp_transport_ = false;
  bool use_shm_transport_ = false;
  rclcpp::TimerBase::SharedPtr auto_discovery_timer_;

  int tx_socket_fd_ = -1;
  int rx_socket_fd_ = -1;
  std::atomic<bool> receiver_running_{false};
  std::thread udp_receiver_thread_;
  std::thread shm_receiver_thread_;
  std::mutex send_mutex_;
  std::atomic<std::uint32_t> next_message_id_{1};
  struct sockaddr_in tx_address_ {};

  static constexpr std::uint32_t kPacketMagic = 0x4D424452;  // "MBDR"
  static constexpr std::uint16_t kPacketVersion = 2;
  static constexpr std::uint16_t kControlChannelId = std::numeric_limits<std::uint16_t>::max();
  static constexpr std::size_t kMaxUdpDatagramBytes = 65507;
  static constexpr std::uint32_t kShmMagic = 0x4D425348;  // "MBSH"
};

}  // namespace ros_middleware_bridge
