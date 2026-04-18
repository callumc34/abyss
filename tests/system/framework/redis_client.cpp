#include "redis_client.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#endif

#include <cerrno>
#include <charconv>
#include <cstring>
#include <ostream>
#include <string>

namespace abyss::system_test {

// --- Reply ------------------------------------------------------------------

Reply Reply::Status(std::string s) {
  Reply r;
  r.type_ = Type::kStatus;
  r.str_ = std::move(s);
  return r;
}

Reply Reply::Error(std::string s) {
  Reply r;
  r.type_ = Type::kError;
  r.str_ = std::move(s);
  return r;
}

Reply Reply::Integer(int64_t n) {
  Reply r;
  r.type_ = Type::kInteger;
  r.int_ = n;
  return r;
}

Reply Reply::Bulk(std::string s) {
  Reply r;
  r.type_ = Type::kBulk;
  r.str_ = std::move(s);
  return r;
}

Reply Reply::Array(std::vector<Reply> elements) {
  Reply r;
  r.type_ = Type::kArray;
  r.elements_ = std::move(elements);
  return r;
}

Reply Reply::Nil() { return Reply{}; }

std::ostream& operator<<(std::ostream& os, Reply::Type t) {
  switch (t) {
    case Reply::Type::kStatus:
      return os << "Status";
    case Reply::Type::kError:
      return os << "Error";
    case Reply::Type::kInteger:
      return os << "Integer";
    case Reply::Type::kBulk:
      return os << "Bulk";
    case Reply::Type::kArray:
      return os << "Array";
    case Reply::Type::kNil:
      return os << "Nil";
  }
  return os << "Unknown";
}

std::ostream& operator<<(std::ostream& os, const Reply& r) {
  os << r.type() << "(";
  switch (r.type()) {
    case Reply::Type::kStatus:
    case Reply::Type::kError:
    case Reply::Type::kBulk:
      os << "\"" << r.String() << "\"";
      break;
    case Reply::Type::kInteger:
      os << r.Integer();
      break;
    case Reply::Type::kArray:
      os << r.Elements().size() << " elements";
      break;
    case Reply::Type::kNil:
      os << "nil";
      break;
  }
  return os << ")";
}

// --- RedisClient ------------------------------------------------------------

RedisClient::~RedisClient() { Close(); }

RedisClient::RedisClient(RedisClient&& other) noexcept
    : fd_(other.fd_), buf_(std::move(other.buf_)), rpos_(other.rpos_), wpos_(other.wpos_) {
  other.fd_ = kInvalidSocket;
}

RedisClient& RedisClient::operator=(RedisClient&& other) noexcept {
  if (this != &other) {
    Close();
    fd_ = other.fd_;
    buf_ = std::move(other.buf_);
    rpos_ = other.rpos_;
    wpos_ = other.wpos_;
    other.fd_ = kInvalidSocket;
  }
  return *this;
}

bool RedisClient::Connect(const std::string& host, uint16_t port,
                          std::chrono::milliseconds timeout) {
  Close();

  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ == kInvalidSocket) return false;

#ifndef _WIN32
#ifdef __APPLE__
  int set = 1;
  setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &set, sizeof(set));
#endif
  // POSIX non-blocking setup
  int flags = fcntl(fd_, F_GETFL, 0);
  fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
#else
  // Windows non-blocking setup
  u_long mode = 1;
  ioctlsocket(fd_, FIONBIO, &mode);
#endif

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

  int ret = connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (ret < 0) {
#ifndef _WIN32
    if (errno != EINPROGRESS) {
      Close();
      return false;
    }
#else
    if (GetSocketError() != WSAEWOULDBLOCK) {
      Close();
      return false;
    }
#endif

    pollfd pfd{};
    pfd.fd = fd_;
    pfd.events = POLLOUT;

#ifdef _WIN32
    int ready = WSAPoll(&pfd, 1, static_cast<int>(timeout.count()));
#else
    int ready = poll(&pfd, 1, static_cast<int>(timeout.count()));
#endif

    if (ready <= 0) {
      Close();
      return false;
    }

    int err = 0;
#ifdef _WIN32
    int errlen = sizeof(err);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &errlen);
#else
    socklen_t errlen = sizeof(err);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &errlen);
#endif
    if (err != 0) {
      Close();
      return false;
    }
  }

#ifndef _WIN32
  fcntl(fd_, F_SETFL, flags);  // Restore blocking mode
#else
  mode = 0;
  ioctlsocket(fd_, FIONBIO, &mode);  // Restore blocking mode
#endif

  buf_.resize(8192);
  rpos_ = 0;
  wpos_ = 0;
  return true;
}

void RedisClient::Close() {
  if (fd_ != kInvalidSocket) {
    CLOSE_SOCKET(fd_);
    fd_ = kInvalidSocket;
  }
  rpos_ = 0;
  wpos_ = 0;
}

std::string RedisClient::Encode(const std::vector<std::string>& args) {
  std::string out;
  out += '*';
  out += std::to_string(args.size());
  out += "\r\n";
  for (const auto& arg : args) {
    out += '$';
    out += std::to_string(arg.size());
    out += "\r\n";
    out += arg;
    out += "\r\n";
  }
  return out;
}

bool RedisClient::SendEncoded(const std::string& data) {
  size_t sent = 0;
  while (sent < data.size()) {
    auto n = send(fd_, reinterpret_cast<const char*>(data.data() + sent),
                  static_cast<int>(data.size() - sent), 0);
    if (n < 0) {
#ifndef _WIN32
      if (errno == EINTR) continue;
#else
      if (GetSocketError() == WSAEINTR) continue;
#endif
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

bool RedisClient::FillBuffer() {
  if (rpos_ > 0 && rpos_ == wpos_) {
    rpos_ = 0;
    wpos_ = 0;
  } else if (rpos_ > buf_.size() / 2) {
    std::memmove(buf_.data(), buf_.data() + rpos_, wpos_ - rpos_);
    wpos_ -= rpos_;
    rpos_ = 0;
  }

  if (wpos_ == buf_.size()) {
    buf_.resize(buf_.size() * 2);
  }

  auto n = recv(fd_, reinterpret_cast<char*>(buf_.data() + wpos_),
                static_cast<int>(buf_.size() - wpos_), 0);
  if (n <= 0) {
#ifndef _WIN32
    if (n < 0 && errno == EINTR) return FillBuffer();
#else
    if (n < 0 && GetSocketError() == WSAEINTR) return FillBuffer();
#endif
    return false;
  }
  wpos_ += static_cast<size_t>(n);
  return true;
}

bool RedisClient::ReadLine(std::string& out) {
  for (;;) {
    for (size_t i = rpos_; i + 1 < wpos_; ++i) {
      if (buf_[i] == '\r' && buf_[i + 1] == '\n') {
        out.assign(buf_.data() + rpos_, i - rpos_);
        rpos_ = i + 2;
        return true;
      }
    }
    if (!FillBuffer()) return false;
  }
}

bool RedisClient::ReadBytes(size_t n, std::string& out) {
  size_t need = n + 2;
  while (wpos_ - rpos_ < need) {
    if (!FillBuffer()) return false;
  }
  out.assign(buf_.data() + rpos_, n);
  rpos_ += need;
  return true;
}

Reply RedisClient::ReadReply() {
  std::string line;
  if (!ReadLine(line)) return Reply::Error("connection lost");
  if (line.empty()) return Reply::Error("empty response");

  char type = line[0];
  std::string_view payload(line.data() + 1, line.size() - 1);

  switch (type) {
    case '+':
      return Reply::Status(std::string(payload));
    case '-':
      return Reply::Error(std::string(payload));
    case ':': {
      int64_t val = 0;
      std::from_chars(payload.data(), payload.data() + payload.size(), val);
      return Reply::Integer(val);
    }
    case '$': {
      int64_t len = 0;
      std::from_chars(payload.data(), payload.data() + payload.size(), len);
      if (len < 0) return Reply::Nil();
      std::string data;
      if (!ReadBytes(static_cast<size_t>(len), data)) return Reply::Error("truncated bulk");
      return Reply::Bulk(std::move(data));
    }
    case '*': {
      int64_t count = 0;
      std::from_chars(payload.data(), payload.data() + payload.size(), count);
      if (count < 0) return Reply::Nil();
      std::vector<Reply> elements;
      elements.reserve(static_cast<size_t>(count));
      for (int64_t i = 0; i < count; ++i) {
        elements.push_back(ReadReply());
      }
      return Reply::Array(std::move(elements));
    }
    default:
      return Reply::Error("unknown RESP type: " + std::string(1, type));
  }
}

Reply RedisClient::Command(std::initializer_list<std::string> args) {
  if (!IsConnected()) return Reply::Error("not connected");
  std::vector<std::string> vec(args);
  if (!SendEncoded(Encode(vec))) return Reply::Error("send failed");
  return ReadReply();
}

std::vector<Reply> RedisClient::Pipeline(const std::vector<std::vector<std::string>>& commands) {
  if (!IsConnected()) return {Reply::Error("not connected")};

  std::string encoded;
  for (const auto& cmd : commands) {
    encoded += Encode(cmd);
  }
  if (!SendEncoded(encoded)) return {Reply::Error("send failed")};

  std::vector<Reply> replies;
  replies.reserve(commands.size());
  for (size_t i = 0; i < commands.size(); ++i) {
    replies.push_back(ReadReply());
  }
  return replies;
}

}  // namespace abyss::system_test
