#pragma once

#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <vector>

namespace abyss::component_test {

// Synchronous RESP2 client over a blocking TCP socket.
class SyncRedisClient {
 public:
  SyncRedisClient() = default;
  ~SyncRedisClient();

  SyncRedisClient(const SyncRedisClient&) = delete;
  SyncRedisClient& operator=(const SyncRedisClient&) = delete;
  SyncRedisClient(SyncRedisClient&&) = delete;
  SyncRedisClient& operator=(SyncRedisClient&&) = delete;

  bool Connect(uint16_t port, std::chrono::milliseconds timeout = std::chrono::milliseconds{2000});
  void Close();
  bool IsConnected() const noexcept { return fd_ >= 0; }

  std::string Command(std::initializer_list<std::string> args);
  bool SendRaw(const std::string& bytes);
  std::string ReadSome(size_t n,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds{500});

  int Fd() const noexcept { return fd_; }

 private:
  static std::string Encode(const std::vector<std::string>& args);

  int fd_ = -1;
};

}  // namespace abyss::component_test
