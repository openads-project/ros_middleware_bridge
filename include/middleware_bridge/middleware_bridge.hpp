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
  enum class TransportType {
    Udp,
    Shm,
  };

  struct ShmChannelHeader;

  struct BridgeChannel {
    std::string subscribe_topic;
    std::string publish_topic;
    std::string topic_type;
    TransportType transport = TransportType::Udp;
    std::string transport_name = "udp";
    std::size_t qos_depth = 10;
    rclcpp::GenericSubscription::SharedPtr subscriber;
    rclcpp::GenericPublisher::SharedPtr publisher;

    int shm_fd = -1;
    void * shm_mapping = nullptr;
    std::size_t shm_mapping_size = 0;
    ShmChannelHeader * shm_header = nullptr;
    std::uint8_t * shm_payload = nullptr;
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
  void receiverLoop();
  void shmReceiverLoop();
  void forwardSerializedMessage(std::size_t channel_index, rclcpp::SerializedMessage & message);
  void stopBackgroundThreads();
  void closeSharedMemoryChannels();

  std::string remote_host_ = "127.0.0.1";
  std::string shm_namespace_ = "middleware_bridge";
  std::string bridge_role_ = "dds";
  int tx_port_ = 17001;
  int rx_port_ = 17002;
  int socket_buffer_bytes_ = 1024 * 1024;
  int max_udp_payload_bytes_ = 60000;
  int max_shm_message_bytes_ = 8 * 1024 * 1024;
  int shm_poll_interval_us_ = 1000;
  int reassembly_timeout_ms_ = 1000;
  std::vector<std::string> dds2zenoh_topics_;
  std::vector<std::string> dds2zenoh_topic_types_;
  std::vector<std::string> dds2zenoh_transports_;
  std::vector<int64_t> dds2zenoh_qos_depths_;
  std::vector<std::string> zenoh2dds_topics_;
  std::vector<std::string> zenoh2dds_topic_types_;
  std::vector<std::string> zenoh2dds_transports_;
  std::vector<int64_t> zenoh2dds_qos_depths_;
  std::vector<BridgeChannel> channels_;
  bool use_udp_transport_ = false;
  bool use_shm_transport_ = false;

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
  static constexpr std::size_t kMaxUdpDatagramBytes = 65507;
  static constexpr std::uint32_t kShmMagic = 0x4D425348;  // "MBSH"
};


}  // namespace middleware_bridge
