#pragma once

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#ifdef _WIN32
// clang-format off
#include <winsock2.h>
#include <ws2tcpip.h>
// clang-format on
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace abyss::testing {

struct HttpTestResponse {
  bool ok = false;
  int status = 0;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
  std::string error;
};

class HttpTestClient {
 public:
  static HttpTestResponse Send(const std::string& host, uint16_t port, const std::string& method,
                               const std::string& path,
                               std::chrono::milliseconds timeout = std::chrono::milliseconds{
                                   5000}) {
    HttpTestResponse out;

#ifdef _WIN32
    using socket_t = SOCKET;
    constexpr socket_t kInvalid = INVALID_SOCKET;
    auto close_sock = [](socket_t s) { closesocket(s); };
#else
    using socket_t = int;
    constexpr socket_t kInvalid = -1;
    auto close_sock = [](socket_t s) { ::close(s); };
#endif

    socket_t sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock == kInvalid) {
      out.error = "socket() failed";
      return out;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
      close_sock(sock);
      out.error = "inet_pton failed";
      return out;
    }

    SetTimeouts(sock, timeout);

    if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      out.error = "connect failed";
      close_sock(sock);
      return out;
    }

    std::string request;
    request.reserve(128 + path.size());
    request += method;
    request += ' ';
    request += path;
    request += " HTTP/1.1\r\nHost: ";
    request += host;
    request += "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

    if (!SendAll(sock, request)) {
      out.error = "send failed";
      close_sock(sock);
      return out;
    }

    std::string response;
    response.reserve(2048);
    std::array<char, 4096> buf{};
    while (true) {
#ifdef _WIN32
      const int n = ::recv(sock, buf.data(), static_cast<int>(buf.size()), 0);
#else
      const auto n = ::recv(sock, buf.data(), buf.size(), 0);
#endif
      if (n > 0) {
        response.append(buf.data(), static_cast<size_t>(n));
        continue;
      }
      if (n == 0) break;
      if (n < 0) {
#ifdef _WIN32
        const int err = WSAGetLastError();
        if (err == WSAEINTR) continue;
#else
        if (errno == EINTR) continue;
#endif
        out.error = "recv failed";
        close_sock(sock);
        return out;
      }
    }

    close_sock(sock);
    return Parse(response);
  }

 private:
  template <typename Sock>
  static void SetTimeouts(Sock sock, std::chrono::milliseconds timeout) {
#ifdef _WIN32
    DWORD ms = static_cast<DWORD>(timeout.count());
    ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&ms), sizeof(ms));
    ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&ms), sizeof(ms));
#else
    timeval tv{};
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count() / 1000);
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>((timeout.count() % 1000) * 1000);
    ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
  }

  template <typename Sock>
  static bool SendAll(Sock sock, std::string_view data) {
    size_t sent = 0;
    while (sent < data.size()) {
#ifdef _WIN32
      const int n = ::send(sock, data.data() + sent, static_cast<int>(data.size() - sent), 0);
#else
      const auto n = ::send(sock, data.data() + sent, data.size() - sent, 0);
#endif
      if (n <= 0) {
#ifdef _WIN32
        if (WSAGetLastError() == WSAEINTR) continue;
#else
        if (errno == EINTR) continue;
#endif
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

  static HttpTestResponse Parse(const std::string& response) {
    HttpTestResponse out;
    if (response.empty()) {
      out.error = "empty response";
      return out;
    }

    const auto header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) {
      out.error = "no header terminator";
      return out;
    }

    const std::string_view head(response.data(), header_end);
    const auto first_line_end = head.find("\r\n");
    if (first_line_end == std::string_view::npos) {
      out.error = "malformed status line";
      return out;
    }

    const std::string_view status_line = head.substr(0, first_line_end);
    const auto sp1 = status_line.find(' ');
    if (sp1 == std::string_view::npos) {
      out.error = "no status code";
      return out;
    }
    const auto sp2 = status_line.find(' ', sp1 + 1);
    const std::string_view code_text = sp2 == std::string_view::npos
                                           ? status_line.substr(sp1 + 1)
                                           : status_line.substr(sp1 + 1, sp2 - sp1 - 1);
    out.status = 0;
    for (char c : code_text) {
      if (c < '0' || c > '9') {
        out.status = 0;
        break;
      }
      out.status = (out.status * 10) + (c - '0');
    }

    std::string_view rest = head.substr(first_line_end + 2);
    while (!rest.empty()) {
      const auto eol = rest.find("\r\n");
      const std::string_view line = eol == std::string_view::npos ? rest : rest.substr(0, eol);
      if (!line.empty()) {
        const auto colon = line.find(':');
        if (colon != std::string_view::npos) {
          std::string key(line.substr(0, colon));
          std::string value(line.substr(colon + 1));
          while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
            value.erase(0, 1);
          }
          out.headers.emplace(std::move(key), std::move(value));
        }
      }
      if (eol == std::string_view::npos) break;
      rest = rest.substr(eol + 2);
    }

    out.body = response.substr(header_end + 4);
    out.ok = out.status > 0;
    return out;
  }
};

}  // namespace abyss::testing
