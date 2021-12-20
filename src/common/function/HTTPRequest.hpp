//
//  HTTPRequest
//

#ifndef HTTPREQUEST_HPP
#define HTTPREQUEST_HPP

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

#if defined(_WIN32) || defined(__CYGWIN__)
#  pragma push_macro("WIN32_LEAN_AND_MEAN")
#  pragma push_macro("NOMINMAX")
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif // WIN32_LEAN_AND_MEAN
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif // NOMINMAX
#  include <winsock2.h>
#  if _WIN32_WINNT < _WIN32_WINNT_WINXP
extern "C" char *_strdup(const char *strSource);
#    define strdup _strdup
#    include <wspiapi.h>
#  endif // _WIN32_WINNT < _WIN32_WINNT_WINXP
#  include <ws2tcpip.h>
#  pragma pop_macro("WIN32_LEAN_AND_MEAN")
#  pragma pop_macro("NOMINMAX")
#else
#  include <errno.h>
#  include <fcntl.h>
#  include <netinet/in.h>
#  include <netdb.h>
#  include <sys/select.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <unistd.h>
#endif // defined(_WIN32) || defined(__CYGWIN__)

namespace http
{
class RequestError final: public std::logic_error
{
 public:
  explicit RequestError(const char* str): std::logic_error{str} {}
  explicit RequestError(const std::string& str): std::logic_error{str} {}
};

class ResponseError final: public std::runtime_error
{
 public:
  explicit ResponseError(const char* str): std::runtime_error{str} {}
  explicit ResponseError(const std::string& str): std::runtime_error{str} {}
};

enum class InternetProtocol: std::uint8_t
{
  V4,
  V6
};

inline namespace detail
{
#if defined(_WIN32) || defined(__CYGWIN__)
class WinSock final
{
 public:
  WinSock()
  {
    WSADATA wsaData;
    const auto error = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (error != 0)
      throw std::system_error(error, std::system_category(), "WSAStartup failed");

    if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2)
    {
      WSACleanup();
      throw std::runtime_error("Invalid WinSock version");
    }

    started = true;
  }

  ~WinSock()
  {
    if (started) WSACleanup();
  }

  WinSock(WinSock&& other) noexcept:
                                      started{other.started}
  {
    other.started = false;
  }

  WinSock& operator=(WinSock&& other) noexcept
  {
    if (&other == this) return *this;
    if (started) WSACleanup();
    started = other.started;
    other.started = false;
    return *this;
  }

 private:
  bool started = false;
};
#endif // defined(_WIN32) || defined(__CYGWIN__)

inline int getLastError() noexcept
{
#if defined(_WIN32) || defined(__CYGWIN__)
  return WSAGetLastError();
#else
  return errno;
#endif // defined(_WIN32) || defined(__CYGWIN__)
}

constexpr int getAddressFamily(const InternetProtocol internetProtocol)
{
  return (internetProtocol == InternetProtocol::V4) ? AF_INET :
         (internetProtocol == InternetProtocol::V6) ? AF_INET6 :
                                                    throw RequestError("Unsupported protocol");
}

class Socket final
{
 public:
#if defined(_WIN32) || defined(__CYGWIN__)
  using Type = SOCKET;
  static constexpr Type invalid = INVALID_SOCKET;
#else
  using Type = int;
  static constexpr Type invalid = -1;
#endif // defined(_WIN32) || defined(__CYGWIN__)

  explicit Socket(const InternetProtocol internetProtocol):
                                                             endpoint{socket(getAddressFamily(internetProtocol), SOCK_STREAM, IPPROTO_TCP)}
  {
    if (endpoint == invalid)
      throw std::system_error(getLastError(), std::system_category(), "Failed to create socket");

#if defined(_WIN32) || defined(__CYGWIN__)
    ULONG mode = 1;
    if (ioctlsocket(endpoint, FIONBIO, &mode) != 0)
    {
      close();
      throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to get socket flags");
    }
#else
    const auto flags = fcntl(endpoint, F_GETFL);
    if (flags == -1)
    {
      close();
      throw std::system_error(errno, std::system_category(), "Failed to get socket flags");
    }

    if (fcntl(endpoint, F_SETFL, flags | O_NONBLOCK) == -1)
    {
      close();
      throw std::system_error(errno, std::system_category(), "Failed to set socket flags");
    }
#endif // defined(_WIN32) || defined(__CYGWIN__)

#ifdef __APPLE__
    const int value = 1;
    if (setsockopt(endpoint, SOL_SOCKET, SO_NOSIGPIPE, &value, sizeof(value)) == -1)
    {
      close();
      throw std::system_error(errno, std::system_category(), "Failed to set socket option");
    }
#endif // __APPLE__
  }

  ~Socket()
  {
    if (endpoint != invalid) close();
  }

  Socket(Socket&& other) noexcept:
                                    endpoint{other.endpoint}
  {
    other.endpoint = invalid;
  }

  Socket& operator=(Socket&& other) noexcept
  {
    if (&other == this) return *this;
    if (endpoint != invalid) close();
    endpoint = other.endpoint;
    other.endpoint = invalid;
    return *this;
  }

  void connect(const struct sockaddr* address, const socklen_t addressSize, const std::int64_t timeout)
  {
#if defined(_WIN32) || defined(__CYGWIN__)
    auto result = ::connect(endpoint, address, addressSize);
    while (result == -1 && WSAGetLastError() == WSAEINTR)
      result = ::connect(endpoint, address, addressSize);

    if (result == -1)
    {
      if (WSAGetLastError() == WSAEWOULDBLOCK)
      {
        select(SelectType::write, timeout);

        char socketErrorPointer[sizeof(int)];
        socklen_t optionLength = sizeof(socketErrorPointer);
        if (getsockopt(endpoint, SOL_SOCKET, SO_ERROR, socketErrorPointer, &optionLength) == -1)
          throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to get socket option");

        int socketError;
        std::memcpy(&socketError, socketErrorPointer, sizeof(socketErrorPointer));

        if (socketError != 0)
          throw std::system_error(socketError, std::system_category(), "Failed to connect");
      }
      else
        throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to connect");
    }
#else
    auto result = ::connect(endpoint, address, addressSize);
    while (result == -1 && errno == EINTR)
      result = ::connect(endpoint, address, addressSize);

    if (result == -1)
    {
      if (errno == EINPROGRESS)
      {
        select(SelectType::write, timeout);

        int socketError;
        socklen_t optionLength = sizeof(socketError);
        if (getsockopt(endpoint, SOL_SOCKET, SO_ERROR, &socketError, &optionLength) == -1)
          throw std::system_error(errno, std::system_category(), "Failed to get socket option");

        if (socketError != 0)
          throw std::system_error(socketError, std::system_category(), "Failed to connect");
      }
      else
        throw std::system_error(errno, std::system_category(), "Failed to connect");
    }
#endif // defined(_WIN32) || defined(__CYGWIN__)
  }

  std::size_t send(const void* buffer, const std::size_t length, const std::int64_t timeout)
  {
    select(SelectType::write, timeout);
#if defined(_WIN32) || defined(__CYGWIN__)
    auto result = ::send(endpoint, reinterpret_cast<const char*>(buffer),
                         static_cast<int>(length), 0);

    while (result == -1 && WSAGetLastError() == WSAEINTR)
      result = ::send(endpoint, reinterpret_cast<const char*>(buffer),
                      static_cast<int>(length), 0);

    if (result == -1)
      throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to send data");
#else
    auto result = ::send(endpoint, reinterpret_cast<const char*>(buffer),
                         length, noSignal);

    while (result == -1 && errno == EINTR)
      result = ::send(endpoint, reinterpret_cast<const char*>(buffer),
                      length, noSignal);

    if (result == -1)
      throw std::system_error(errno, std::system_category(), "Failed to send data");
#endif // defined(_WIN32) || defined(__CYGWIN__)
    return static_cast<std::size_t>(result);
  }

  std::size_t recv(void* buffer, const std::size_t length, const std::int64_t timeout)
  {
    select(SelectType::read, timeout);
#if defined(_WIN32) || defined(__CYGWIN__)
    auto result = ::recv(endpoint, reinterpret_cast<char*>(buffer),
                         static_cast<int>(length), 0);

    while (result == -1 && WSAGetLastError() == WSAEINTR)
      result = ::recv(endpoint, reinterpret_cast<char*>(buffer),
                      static_cast<int>(length), 0);

    if (result == -1)
      throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to read data");
#else
    auto result = ::recv(endpoint, reinterpret_cast<char*>(buffer),
                         length, noSignal);

    while (result == -1 && errno == EINTR)
      result = ::recv(endpoint, reinterpret_cast<char*>(buffer),
                      length, noSignal);

    if (result == -1)
      throw std::system_error(errno, std::system_category(), "Failed to read data");
#endif // defined(_WIN32) || defined(__CYGWIN__)
    return static_cast<std::size_t>(result);
  }

 private:
  enum class SelectType
  {
    read,
    write
  };

  void select(const SelectType type, const std::int64_t timeout)
  {
    fd_set descriptorSet;
    FD_ZERO(&descriptorSet);
    FD_SET(endpoint, &descriptorSet);

#if defined(_WIN32) || defined(__CYGWIN__)
    TIMEVAL selectTimeout{
        static_cast<LONG>(timeout / 1000),
        static_cast<LONG>((timeout % 1000) * 1000)
    };
    auto count = ::select(0,
                          (type == SelectType::read) ? &descriptorSet : nullptr,
                          (type == SelectType::write) ? &descriptorSet : nullptr,
                          nullptr,
                          (timeout >= 0) ? &selectTimeout : nullptr);

    while (count == -1 && WSAGetLastError() == WSAEINTR)
      count = ::select(0,
                       (type == SelectType::read) ? &descriptorSet : nullptr,
                       (type == SelectType::write) ? &descriptorSet : nullptr,
                       nullptr,
                       (timeout >= 0) ? &selectTimeout : nullptr);

    if (count == -1)
      throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to select socket");
    else if (count == 0)
      throw ResponseError("Request timed out");
#else
    timeval selectTimeout{
        static_cast<time_t>(timeout / 1000),
        static_cast<suseconds_t>((timeout % 1000) * 1000)
    };
    auto count = ::select(endpoint + 1,
                          (type == SelectType::read) ? &descriptorSet : nullptr,
                          (type == SelectType::write) ? &descriptorSet : nullptr,
                          nullptr,
                          (timeout >= 0) ? &selectTimeout : nullptr);

    while (count == -1 && errno == EINTR)
      count = ::select(endpoint + 1,
                       (type == SelectType::read) ? &descriptorSet : nullptr,
                       (type == SelectType::write) ? &descriptorSet : nullptr,
                       nullptr,
                       (timeout >= 0) ? &selectTimeout : nullptr);

    if (count == -1)
      throw std::system_error(errno, std::system_category(), "Failed to select socket");
    else if (count == 0)
      throw ResponseError("Request timed out");
#endif // defined(_WIN32) || defined(__CYGWIN__)
  }

  void close() noexcept
  {
#if defined(_WIN32) || defined(__CYGWIN__)
    closesocket(endpoint);
#else
    ::close(endpoint);
#endif // defined(_WIN32) || defined(__CYGWIN__)
  }

#if defined(__unix__) && !defined(__APPLE__) && !defined(__CYGWIN__)
  static constexpr int noSignal = MSG_NOSIGNAL;
#else
  static constexpr int noSignal = 0;
#endif // defined(__unix__) && !defined(__APPLE__)

  Type endpoint = invalid;
};
}

    struct Response final
{
  // RFC 7231, 6. Response Status Codes
  enum Status
  {
    Continue = 100,
    SwitchingProtocol = 101,
    Processing = 102,
    EarlyHints = 103,

    Ok = 200,
    Created = 201,
    Accepted = 202,
    NonAuthoritativeInformation = 203,
    NoContent = 204,
    ResetContent = 205,
    PartialContent = 206,
    MultiStatus = 207,
    AlreadyReported = 208,
    ImUsed = 226,

    MultipleChoice = 300,
    MovedPermanently = 301,
    Found = 302,
    SeeOther = 303,
    NotModified = 304,
    UseProxy = 305,
    TemporaryRedirect = 307,
    PermanentRedirect = 308,

    BadRequest = 400,
    Unauthorized = 401,
    PaymentRequired = 402,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    NotAcceptable = 406,
    ProxyAuthenticationRequired = 407,
    RequestTimeout = 408,
    Conflict = 409,
    Gone = 410,
    LengthRequired = 411,
    PreconditionFailed = 412,
    PayloadTooLarge = 413,
    UriTooLong = 414,
    UnsupportedMediaType = 415,
    RangeNotSatisfiable = 416,
    ExpectationFailed = 417,
    MisdirectedRequest = 421,
    UnprocessableEntity = 422,
    Locked = 423,
    FailedDependency = 424,
    TooEarly = 425,
    UpgradeRequired = 426,
    PreconditionRequired = 428,
    TooManyRequests = 429,
    RequestHeaderFieldsTooLarge = 431,
    UnavailableForLegalReasons = 451,

    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504,
    HttpVersionNotSupported = 505,
    VariantAlsoNegotiates = 506,
    InsufficientStorage = 507,
    LoopDetected = 508,
    NotExtended = 510,
    NetworkAuthenticationRequired = 511
  };

  int status = 0;
  std::string description;
  std::vector<std::string> headers;
  std::vector<std::uint8_t> body;
};

class Request final
{
 public:
  explicit Request(const std::string& url,
                   const InternetProtocol protocol = InternetProtocol::V4):
                                                                             internetProtocol{protocol}
  {
    const auto schemeEndPosition = url.find("://");

    if (schemeEndPosition != std::string::npos)
    {
      scheme = url.substr(0, schemeEndPosition);
      path = url.substr(schemeEndPosition + 3);
    }
    else
    {
      scheme = "http";
      path = url;
    }

    const auto fragmentPosition = path.find('#');

    // remove the fragment part
    if (fragmentPosition != std::string::npos)
      path.resize(fragmentPosition);

    const auto pathPosition = path.find('/');

    if (pathPosition == std::string::npos)
    {
      host = path;
      path = "/";
    }
    else
    {
      host = path.substr(0, pathPosition);
      path = path.substr(pathPosition);
    }

    const auto portPosition = host.find(':');

    if (portPosition != std::string::npos)
    {
      domain = host.substr(0, portPosition);
      port = host.substr(portPosition + 1);
    }
    else
    {
      domain = host;
      port = "80";
    }
  }

  Response send(const std::string& method = "GET",
                const std::string& body = "",
                const std::vector<std::string>& headers = {},
                const std::chrono::milliseconds timeout = std::chrono::milliseconds{-1})
  {
    return send(method,
                std::vector<uint8_t>(body.begin(), body.end()),
                headers,
                timeout);
  }

  Response send(const std::string& method,
                const std::vector<uint8_t>& body,
                const std::vector<std::string>& headers,
                const std::chrono::milliseconds timeout = std::chrono::milliseconds{-1})
  {
    const auto stopTime = std::chrono::steady_clock::now() + timeout;

    if (scheme != "http")
      throw RequestError("Only HTTP scheme is supported");

    addrinfo hints = {};
    hints.ai_family = getAddressFamily(internetProtocol);
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* info;
    if (getaddrinfo(domain.c_str(), port.c_str(), &hints, &info) != 0)
      throw std::system_error(getLastError(), std::system_category(), "Failed to get address info of " + domain);

    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> addressInfo{info, freeaddrinfo};

    // RFC 7230, 3.1.1. Request Line
    std::string headerData = method + " " + path + " HTTP/1.1\r\n";

    for (const auto& header : headers)
      headerData += header + "\r\n";

    // RFC 7230, 3.2. Header Fields
    headerData += "Host: " + host + "\r\n"
                  "Content-Length: " + std::to_string(body.size()) + "\r\n"
                  "\r\n";

    std::vector<uint8_t> requestData(headerData.begin(), headerData.end());
    requestData.insert(requestData.end(), body.begin(), body.end());

    Socket socket{internetProtocol};

    // take the first address from the list
    socket.connect(addressInfo->ai_addr, static_cast<socklen_t>(addressInfo->ai_addrlen),
                   (timeout.count() >= 0) ? getRemainingMilliseconds(stopTime) : -1);

    auto remaining = requestData.size();
    auto sendData = requestData.data();

    // send the request
    while (remaining > 0)
    {
      const auto size = socket.send(sendData, remaining,
                                    (timeout.count() >= 0) ? getRemainingMilliseconds(stopTime) : -1);
      remaining -= size;
      sendData += size;
    }

    std::array<std::uint8_t, 4096> tempBuffer;
    constexpr std::array<std::uint8_t, 2> crlf = {'\r', '\n'};
    Response response;
    std::vector<std::uint8_t> responseData;
    enum class State
    {
      parsingStatusLine,
      parsingHeaders,
      parsingBody
    } state = State::parsingStatusLine;
    bool contentLengthReceived = false;
    std::size_t contentLength = 0;
    bool chunkedResponse = false;
    std::size_t expectedChunkSize = 0;
    bool removeCrlfAfterChunk = false;

    // read the response
    for (;;)
    {
      const auto size = socket.recv(tempBuffer.data(), tempBuffer.size(),
                                    (timeout.count() >= 0) ? getRemainingMilliseconds(stopTime) : -1);
      if (size == 0) // disconnected
        return response;

      responseData.insert(responseData.end(), tempBuffer.begin(), tempBuffer.begin() + size);

      if (state != State::parsingBody)
        for (;;)
        {
          // RFC 7230, 3. Message Format
          const auto i = std::search(responseData.begin(), responseData.end(), crlf.begin(), crlf.end());

          // didn't find a newline
          if (i == responseData.end()) break;

          const std::string line(responseData.begin(), i);
          responseData.erase(responseData.begin(), i + 2);

          // empty line indicates the end of the header section (RFC 7230, 2.1. Client/Server Messaging)
          if (line.empty())
          {
            state = State::parsingBody;
            break;
          }
          else if (state == State::parsingStatusLine) // RFC 7230, 3.1.2. Status Line
          {
            state = State::parsingHeaders;

            const auto httpEndIterator = std::find(line.begin(), line.end(), ' ');

            if (httpEndIterator != line.end())
            {
              const auto statusStartIterator = httpEndIterator + 1;
              const auto statusEndIterator = std::find(statusStartIterator, line.end(), ' ');
              const std::string status{statusStartIterator, statusEndIterator};
              response.status = std::stoi(status);

              if (statusEndIterator != line.end())
              {
                const auto descriptionStartIterator = statusEndIterator + 1;
                response.description = std::string{descriptionStartIterator, line.end()};
              }
            }
          }
          else if (state == State::parsingHeaders) // RFC 7230, 3.2. Header Fields
          {
            response.headers.push_back(line);

            const auto colonPosition = line.find(':');

            if (colonPosition == std::string::npos)
              throw ResponseError("Invalid header: " + line);

            auto headerName = line.substr(0, colonPosition);

            const auto toLower = [](const char c) {
              return (c >= 'A' && c <= 'Z') ? c - ('A' - 'a') : c;
            };

            std::transform(headerName.begin(), headerName.end(), headerName.begin(), toLower);

            auto headerValue = line.substr(colonPosition + 1);

            // RFC 7230, Appendix B. Collected ABNF
            const auto isNotWhitespace = [](const char c){
              return c != ' ' && c != '\t';
            };

            // ltrim
            headerValue.erase(headerValue.begin(), std::find_if(headerValue.begin(), headerValue.end(), isNotWhitespace));

            // rtrim
            headerValue.erase(std::find_if(headerValue.rbegin(), headerValue.rend(), isNotWhitespace).base(), headerValue.end());

            if (headerName == "content-length")
            {
              contentLength = std::stoul(headerValue);
              contentLengthReceived = true;
              response.body.reserve(contentLength);
            }
            else if (headerName == "transfer-encoding")
            {
              if (headerValue == "chunked")
                chunkedResponse = true;
              else
                throw ResponseError("Unsupported transfer encoding: " + headerValue);
            }
          }
        }

      if (state == State::parsingBody)
      {
        // Content-Length must be ignored if Transfer-Encoding is received (RFC 7230, 3.2. Content-Length)
        if (chunkedResponse)
        {
          for (;;)
          {
            if (expectedChunkSize > 0)
            {
              const auto toWrite = (std::min)(expectedChunkSize, responseData.size());
              response.body.insert(response.body.end(), responseData.begin(), responseData.begin() + static_cast<std::ptrdiff_t>(toWrite));
              responseData.erase(responseData.begin(), responseData.begin() + static_cast<std::ptrdiff_t>(toWrite));
              expectedChunkSize -= toWrite;

              if (expectedChunkSize == 0) removeCrlfAfterChunk = true;
              if (responseData.empty()) break;
            }
            else
            {
              if (removeCrlfAfterChunk)
              {
                if (responseData.size() < 2) break;

                if (!std::equal(crlf.begin(), crlf.end(), responseData.begin()))
                  throw ResponseError("Invalid chunk");

                removeCrlfAfterChunk = false;
                responseData.erase(responseData.begin(), responseData.begin() + 2);
              }

              const auto i = std::search(responseData.begin(), responseData.end(), crlf.begin(), crlf.end());

              if (i == responseData.end()) break;

              const std::string line(responseData.begin(), i);
              responseData.erase(responseData.begin(), i + 2);

              expectedChunkSize = std::stoul(line, nullptr, 16);
              if (expectedChunkSize == 0)
                return response;
            }
          }
        }
        else
        {
          response.body.insert(response.body.end(), responseData.begin(), responseData.end());
          responseData.clear();

          // got the whole content
          if (contentLengthReceived && response.body.size() >= contentLength)
            return response;
        }
      }
    }

    return response;
  }

 private:
  static std::int64_t getRemainingMilliseconds(const std::chrono::steady_clock::time_point time) noexcept
  {
    const auto now = std::chrono::steady_clock::now();
    const auto remainingTime = std::chrono::duration_cast<std::chrono::milliseconds>(time - now);
    return (remainingTime.count() > 0) ? remainingTime.count() : 0;
  }

#if defined(_WIN32) || defined(__CYGWIN__)
  WinSock winSock;
#endif // defined(_WIN32) || defined(__CYGWIN__)
  InternetProtocol internetProtocol;
  std::string scheme;
  std::string host;
  std::string domain;
  std::string port;
  std::string path;
};
}

#endif // HTTPREQUEST_HPP