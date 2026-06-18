// Copyright Institute for Automotive Engineering (ika), RWTH Aachen University
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <netinet/in.h>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
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
  MiddlewareBridge();
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

  void declareAndLoadParameters();
  void setupBridgeChannels();
  void setupSockets();
  void setupSharedMemoryChannels();
  void setupSharedMemoryChannel(BridgeChannel& channel, const std::string& ns, std::size_t channel_index);
  void runAutoDiscoveryScan();
  void refreshLocalSourceQos();
  void announceStaticSourceChannels();
  std::size_t addChannelIfMissing(bool is_side_a_to_b,
                                  const std::string& topic_name,
                                  const std::string& topic_type,
                                  const std::string& transport,
                                  const BridgeQosProfile& qos,
                                  bool from_auto_discovery,
                                  bool* added = nullptr);
  BridgeQosProfile defaultQosForTopic(const std::string& topic_name, std::size_t fallback_depth) const;
  BridgeQosProfile resolveSourceQos(const std::string& topic_name, std::size_t fallback_depth) const;
  static bool qosProfilesEqual(const BridgeQosProfile& lhs, const BridgeQosProfile& rhs);
  rclcpp::QoS makeRclcppQos(const BridgeQosProfile& qos) const;
  void createChannelEndpoints(BridgeChannel& channel, std::size_t channel_index);
  void updateChannelQos(std::size_t channel_index, const BridgeQosProfile& qos, const char* reason);
  void sendUdpPayload(std::uint16_t channel_id, const std::uint8_t* payload, std::size_t payload_size);
  void announceAutoDiscoveredChannel(std::uint16_t channel_id,
                                     bool is_side_a_to_b,
                                     const std::string& topic_name,
                                     const std::string& topic_type,
                                     const std::string& transport,
                                     const BridgeQosProfile& qos);
  void handleAutoDiscoveryAnnouncement(const std::uint8_t* payload, std::size_t payload_size);
  bool aggregateTfStaticMessage(std::size_t channel_index,
                                rclcpp::SerializedMessage& message,
                                rclcpp::SerializedMessage& aggregated_message);
  void receiverLoop();
  void shmReceiverLoop();
  void forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage& message);
  void stopBackgroundThreads();
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
  struct sockaddr_in tx_address_{};

  static constexpr std::uint32_t kPacketMagic = 0x4D424452;  // "MBDR"
  static constexpr std::uint16_t kPacketVersion = 2;
  static constexpr std::uint16_t kControlChannelId = std::numeric_limits<std::uint16_t>::max();
  static constexpr std::size_t kMaxUdpDatagramBytes = 65507;
  static constexpr std::uint32_t kShmMagic = 0x4D425348;  // "MBSH"
};

}  // namespace ros_middleware_bridge
