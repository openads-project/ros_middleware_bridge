#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

#include <rclcpp/generic_publisher.hpp>
#include <rclcpp/generic_subscription.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialized_message.hpp>


namespace middleware_bridge {

class MiddlewareBridge : public rclcpp::Node {
 public:
  MiddlewareBridge();
  ~MiddlewareBridge() override;

  int num_threads_ = 1;

 private:
  struct BridgeChannel {
    std::string subscribe_topic;
    std::string publish_topic;
    std::string topic_type;
    std::size_t qos_depth = 10;
    rclcpp::GenericSubscription::SharedPtr subscriber;
    rclcpp::GenericPublisher::SharedPtr publisher;
  };

  struct PacketHeader {
    std::uint32_t magic;
    std::uint16_t version;
    std::uint16_t channel_index;
    std::uint32_t payload_size;
  };

  void declareAndLoadParameters();
  void setupBridgeChannels();
  void setupSockets();
  void receiverLoop();
  void forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage & message);
  void stopReceiverThread();

  std::string remote_host_ = "127.0.0.1";
  std::string bridge_role_ = "dds";
  int tx_port_ = 17001;
  int rx_port_ = 17002;
  int socket_buffer_bytes_ = 1024 * 1024;
  std::vector<std::string> dds2zenoh_topics_;
  std::vector<std::string> dds2zenoh_topic_types_;
  std::vector<int64_t> dds2zenoh_qos_depths_;
  std::vector<std::string> zenoh2dds_topics_;
  std::vector<std::string> zenoh2dds_topic_types_;
  std::vector<int64_t> zenoh2dds_qos_depths_;
  std::vector<BridgeChannel> channels_;

  int tx_socket_fd_ = -1;
  int rx_socket_fd_ = -1;
  std::atomic<bool> receiver_running_{false};
  std::thread receiver_thread_;
  std::mutex send_mutex_;
  struct sockaddr_in tx_address_ {};

  static constexpr std::uint32_t kPacketMagic = 0x4D424452;  // "MBDR"
  static constexpr std::uint16_t kPacketVersion = 1;
  static constexpr std::size_t kMaxUdpDatagramBytes = 65507;
};


}  // namespace middleware_bridge
