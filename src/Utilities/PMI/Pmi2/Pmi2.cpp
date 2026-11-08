/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "Pmi2.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "crane/Logger.h"
#include "fmt/format.h"
#include "protos/Pmi2.grpc.pb.h"

namespace pmi2 {
namespace {
using Fields = std::map<std::string, std::string>;
constexpr size_t kMaxFrame = 64 * 1024;
constexpr size_t kMaxKey = 64;
constexpr size_t kMaxValue = 1024;

std::optional<uint32_t> Number(std::string_view value) {
  uint32_t result;
  auto [end, ec] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (ec != std::errc{} || end != value.data() + value.size())
    return std::nullopt;
  return result;
}

std::optional<Fields> Parse(std::string_view body, char delimiter) {
  Fields fields;
  if (body.find('\0') != std::string_view::npos) return std::nullopt;
  while (!body.empty()) {
    std::string pair;
    size_t pos = 0;
    while (pos < body.size()) {
      char ch = body[pos++];
      if (ch == delimiter) {
        // PMI-2 doubles semicolons inside values. Splitting first would lose
        // business-card data used by MPI implementations to exchange addresses.
        if (delimiter != ';' || pos == body.size() || body[pos] != ';') break;
        ++pos;
      }
      pair += ch;
    }
    if (!pair.empty()) {
      auto eq = pair.find('=');
      if (eq == std::string::npos || eq == 0 ||
          !fields.emplace(pair.substr(0, eq), pair.substr(eq + 1)).second)
        return std::nullopt;
    }
    body.remove_prefix(pos);
  }
  if (!fields.contains("cmd")) return std::nullopt;
  return fields;
}

std::string Mapping(const crane::grpc::StepToD& step) {
  struct Block {
    uint32_t first_node;
    uint32_t node_count;
    int ranks_per_node;
  };
  std::vector<Block> blocks;
  // Consecutive equally sized nodes share one tuple, keeping large regular
  // allocations within PMI2_MAX_VALLEN. Repeated runs still encode cyclic or
  // nonuniform layouts without assuming a particular scheduler distribution.
  for (int rank = 0; rank < step.task_node_list_size();) {
    auto node = step.task_node_list(rank);
    int begin = rank++;
    while (rank < step.task_node_list_size() &&
           step.task_node_list(rank) == node)
      ++rank;
    int count = rank - begin;
    if (!blocks.empty() &&
        blocks.back().first_node + blocks.back().node_count == node &&
        blocks.back().ranks_per_node == count)
      ++blocks.back().node_count;
    else
      blocks.push_back({node, 1, count});
  }
  std::string result = "(vector";
  for (const auto& block : blocks)
    result += fmt::format(",({},{},{})", block.first_node, block.node_count,
                          block.ranks_per_node);
  result += ')';
  return result;
}
}  // namespace

struct Server::Impl : crane::grpc::pmi2::Pmi2::Service {
  struct Client {
    int fd{-1};
    uint32_t rank{};
    bool negotiated{false};
    bool initialized{false};
    bool finalized{false};
    bool closing{false};
    std::string input;
    std::string output;
    std::optional<Fields> waiting;
    std::chrono::steady_clock::time_point deadline;
    ~Client() {
      if (fd >= 0) close(fd);
    }
  };
  mpi::Config config;
  crane::grpc::StepToD step;
  std::string jobid;
  std::string mapping;
  uint32_t node{};
  size_t local_size{};
  std::map<uint32_t, std::unique_ptr<Client>> clients;
  Fields pending_kvs;
  Fields visible_kvs;
  Fields node_attrs;
  std::mutex mutex;
  std::condition_variable cv;
  std::thread io_thread;
  std::thread fence_thread;
  std::unique_ptr<grpc::Server> rpc_server;
  std::string port;
  std::map<CranedId, std::shared_ptr<crane::grpc::pmi2::Pmi2::Stub>> peers;
  std::map<CranedId, std::string> peer_ports;
  std::optional<CranedId> parent;
  std::set<CranedId> children;
  std::map<CranedId, Fields> contributions;
  uint64_t epoch{0};
  std::optional<uint64_t> completed_epoch;
  Fields completed_kvs;
  std::optional<std::chrono::steady_clock::time_point> fence_deadline;
  std::unique_ptr<grpc::ClientContext> outgoing_context;
  int wake_fd{-1};
  bool stopping{false};
  bool failed{false};
  bool failure_reported{false};
  std::function<void()> on_failure;
  std::chrono::seconds timeout;

  ~Impl() {
    {
      std::lock_guard lock(mutex);
      stopping = true;
      if (outgoing_context) outgoing_context->TryCancel();
      Wake();
      cv.notify_all();
    }
    // Do not hold the state lock while gRPC joins handlers waiting on that
    // lock.
    if (rpc_server) rpc_server->Shutdown(std::chrono::system_clock::now());
    if (fence_thread.joinable()) fence_thread.join();
    if (io_thread.joinable()) io_thread.join();
    if (wake_fd >= 0) close(wake_fd);
  }
  void Wake() const {
    uint64_t one = 1;
    if (wake_fd >= 0) (void)write(wake_fd, &one, sizeof(one));
  }
  void Fail(std::string_view reason) {
    if (failed || stopping) return;
    CRANE_ERROR("[Step #{}.{}] PMI-2 failure: {}", step.job_id(),
                step.step_id(), reason);
    failed = true;
    cv.notify_all();
    Wake();
  }
  void Reply(Client& c, const Fields& request, Fields fields, int rc = 0) {
    fields["cmd"] = request.at("cmd") + "-response";
    fields["rc"] = std::to_string(rc);
    if (auto it = request.find("thrid"); it != request.end())
      fields["thrid"] = it->second;
    std::string body = "cmd=" + fields.at("cmd") + ';';
    fields.erase("cmd");
    for (const auto& [key, value] : fields) {
      body += key + '=';
      for (char ch : value) {
        body += ch;
        if (ch == ';') body += ch;
      }
      body += ';';
    }
    if (body.size() > kMaxFrame ||
        c.output.size() + body.size() > 2 * kMaxFrame) {
      Fail("PMI-2 response exceeds the protocol limit");
      return;
    }
    c.output += fmt::format("{:6}", body.size()) + body;
    Wake();
  }
  void Attribute(Client& c, const Fields& request, const Fields& attributes) {
    auto it = attributes.find(request.at("key"));
    Fields response{{"found", it == attributes.end() ? "FALSE" : "TRUE"}};
    if (it != attributes.end()) response["value"] = it->second;
    Reply(c, request, std::move(response));
  }
  size_t LocalArrivals() const {
    size_t arrived = 0;
    for (const auto& [rank, c] : clients)
      if (c->waiting && c->waiting->at("cmd") == "kvs-fence") ++arrived;
    return arrived;
  }
  void StartFenceDeadline() {
    if (!fence_deadline)
      fence_deadline = std::chrono::steady_clock::now() + timeout;
    cv.notify_all();
  }
  grpc::Status Fence(grpc::ServerContext* context,
                     const crane::grpc::pmi2::FenceRequest* request,
                     crane::grpc::pmi2::FenceReply* reply) override {
    std::unique_lock lock(mutex);
    if (request->job_id() != step.job_id() ||
        request->step_id() != step.step_id() ||
        !children.contains(request->craned_id()))
      return {grpc::StatusCode::INVALID_ARGUMENT,
              "unexpected step or tree member"};
    if (completed_epoch == request->epoch()) {
      reply->set_ok(true);
      reply->mutable_kvs()->insert(completed_kvs.begin(), completed_kvs.end());
      return grpc::Status::OK;
    }
    if (request->epoch() != epoch)
      return {grpc::StatusCode::FAILED_PRECONDITION, "unexpected fence epoch"};
    Fields incoming(request->kvs().begin(), request->kvs().end());
    for (const auto& [key, value] : incoming)
      if (key.empty() || key.size() >= kMaxKey || value.size() >= kMaxValue)
        return {grpc::StatusCode::INVALID_ARGUMENT, "invalid KVS entry"};
    auto [it, inserted] = contributions.emplace(request->craned_id(), incoming);
    if (!inserted && it->second != incoming)
      return {grpc::StatusCode::INVALID_ARGUMENT, "conflicting fence retry"};
    StartFenceDeadline();
    auto deadline = std::chrono::steady_clock::now() + timeout;
    if (!cv.wait_until(lock, deadline, [&] {
          return failed || stopping || completed_epoch == request->epoch();
        }))
      Fail("child fence timed out");
    if (failed || stopping || context->IsCancelled())
      return {grpc::StatusCode::CANCELLED, "step fence did not complete"};
    reply->set_ok(true);
    reply->mutable_kvs()->insert(completed_kvs.begin(), completed_kvs.end());
    return grpc::Status::OK;
  }
  grpc::Status Abort(grpc::ServerContext*,
                     const crane::grpc::pmi2::AbortRequest* request,
                     crane::grpc::pmi2::AbortReply* reply) override {
    std::lock_guard lock(mutex);
    if (request->job_id() != step.job_id() ||
        request->step_id() != step.step_id() ||
        !peers.contains(request->craned_id()))
      return {grpc::StatusCode::INVALID_ARGUMENT, "unexpected step or peer"};
    Fail("peer reported a step failure");
    reply->set_ok(true);
    return grpc::Status::OK;
  }
  void FenceLoop() {
    std::unique_lock lock(mutex);
    while (!stopping && !failed) {
      cv.wait_for(lock, std::chrono::milliseconds(100), [&] {
        return stopping || failed ||
               (LocalArrivals() == local_size &&
                contributions.size() == children.size() &&
                peers.size() + 1 == static_cast<size_t>(step.nodelist_size()));
      });
      if (stopping || failed) break;
      if (fence_deadline &&
          std::chrono::steady_clock::now() >= *fence_deadline) {
        Fail("step fence timed out");
        break;
      }
      if (LocalArrivals() != local_size ||
          contributions.size() != children.size() ||
          peers.size() + 1 != static_cast<size_t>(step.nodelist_size()))
        continue;
      auto round = epoch;
      auto delta = pending_kvs;
      for (const auto& [id, data] : contributions)
        for (const auto& [key, value] : data) delta[key] = value;
      if (parent) {
        auto stub = peers.at(*parent);
        crane::grpc::pmi2::FenceRequest request;
        crane::grpc::pmi2::FenceReply reply;
        request.set_job_id(step.job_id());
        request.set_step_id(step.step_id());
        request.set_craned_id(config.CranedIdOfThisNode);
        request.set_epoch(round);
        request.mutable_kvs()->insert(delta.begin(), delta.end());
        outgoing_context = std::make_unique<grpc::ClientContext>();
        outgoing_context->set_deadline(std::chrono::system_clock::now() +
                                       timeout);
        auto* context = outgoing_context.get();
        lock.unlock();
        auto status = stub->Fence(context, request, &reply);
        lock.lock();
        outgoing_context.reset();
        if (stopping || failed) break;
        if (!status.ok() || !reply.ok()) {
          Fail("parent fence failed");
          break;
        }
        visible_kvs = Fields(reply.kvs().begin(), reply.kvs().end());
      } else {
        for (const auto& [key, value] : delta) visible_kvs[key] = value;
      }
      // Publish the complete snapshot before releasing either local clients or
      // child RPCs. KVS_Put data is intentionally invisible until this point.
      completed_kvs = visible_kvs;
      completed_epoch = round;
      ++epoch;
      pending_kvs.clear();
      contributions.clear();
      fence_deadline.reset();
      for (auto& [rank, c] : clients) {
        Reply(*c, *c->waiting, {});
        c->waiting.reset();
      }
      cv.notify_all();
    }
  }
  void Request(Client& c, const Fields& request) {
    const auto& cmd = request.at("cmd");
    auto has = [&](std::string_view key) {
      return request.contains(std::string(key));
    };
    if (cmd == "fullinit" && !c.initialized) {
      if (!has("pmirank") || Number(request.at("pmirank")) != c.rank) {
        Reply(c, request,
              {{"errmsg", "rank does not match the assigned channel"}}, 3);
        return;
      }
      c.initialized = true;
      Reply(c, request,
            {{"pmi-version", "2"},
             {"pmi-subversion", "0"},
             {"rank", std::to_string(c.rank)},
             {"size", std::to_string(step.task_node_list_size())},
             {"appnum", "0"},
             {"debugged", "FALSE"},
             {"pmiverbose", "FALSE"}});
      return;
    }
    if (!c.initialized || c.finalized || c.waiting) {
      Fail("request out of order");
      return;
    }
    if (cmd == "finalize") {
      c.finalized = true;
      c.closing = true;
      Reply(c, request, {});
    } else if (cmd == "abort") {
      Fail("client requested Abort");
    } else if (cmd == "job-getid") {
      Reply(c, request, {{"jobid", jobid}});
    } else if (cmd == "kvs-put" || cmd == "info-putnodeattr") {
      if (!has("key") || !has("value") || request.at("key").empty() ||
          request.at("key").size() >= kMaxKey ||
          request.at("value").size() >= kMaxValue) {
        Reply(c, request, {{"errmsg", "invalid key or value length"}}, 3);
        return;
      }
      auto& dest = cmd == "kvs-put" ? pending_kvs : node_attrs;
      dest[request.at("key")] = request.at("value");
      Reply(c, request, {});
      if (cmd == "info-putnodeattr") {
        for (auto& [rank, waiting] : clients) {
          if (waiting->waiting &&
              waiting->waiting->at("cmd") == "info-getnodeattr" &&
              node_attrs.contains(waiting->waiting->at("key"))) {
            Attribute(*waiting, *waiting->waiting, node_attrs);
            waiting->waiting.reset();
          }
        }
      }
    } else if (cmd == "kvs-get") {
      if (!has("key") || !has("jobid") || request.at("jobid") != jobid) {
        Reply(c, request, {{"errmsg", "invalid job or key"}}, 3);
        return;
      }
      auto it = visible_kvs.find(request.at("key"));
      if (it == visible_kvs.end())
        Reply(c, request, {{"errmsg", "key not found"}}, 4);
      else
        Reply(c, request, {{"value", it->second}, {"found", "TRUE"}});
    } else if (cmd == "kvs-fence") {
      c.waiting = request;
      c.deadline = std::chrono::steady_clock::now() + timeout;
      StartFenceDeadline();
    } else if (cmd == "info-getjobattr" || cmd == "info-getnodeattr") {
      if (!has("key")) {
        Reply(c, request, {}, 3);
        return;
      }
      if (cmd == "info-getjobattr") {
        Fields attributes{
            {"PMI_process_mapping", mapping},
            {"universeSize", std::to_string(step.task_node_list_size())}};
        Attribute(c, request, attributes);
      } else if (!node_attrs.contains(request.at("key")) && has("wait") &&
                 request.at("wait") == "TRUE") {
        c.waiting = request;
        c.deadline = std::chrono::steady_clock::now() + timeout;
      } else {
        Attribute(c, request, node_attrs);
      }
    } else {
      // Static MPI must receive an error, rather than hanging on operations
      // such as Spawn/Connect that require a different scheduler lifecycle.
      Reply(c, request, {{"errmsg", "operation not supported"}}, 14);
    }
  }
  void Decode(Client& c) {
    while (!c.input.empty() && !failed && !c.closing) {
      if (!c.negotiated) {
        auto end = c.input.find('\n');
        if (end == std::string::npos) {
          if (c.input.size() > 1024) Fail("oversized PMI initialization");
          return;
        }
        auto request = Parse(std::string_view(c.input).substr(0, end), ' ');
        if (!request || request->at("cmd") != "init") {
          Fail("invalid PMI initialization");
          return;
        }
        bool supported = request->contains("pmi_version") &&
                         request->at("pmi_version") == "2" &&
                         request->contains("pmi_subversion") &&
                         request->at("pmi_subversion") == "0";
        c.output += fmt::format(
            "cmd=response_to_init rc={} pmi_version=2 pmi_subversion=0\n",
            supported ? 0 : 1);
        c.input.erase(0, end + 1);
        c.negotiated = true;
        c.closing = !supported;
      } else {
        if (c.input.size() < 6) return;
        auto header = std::string_view(c.input).substr(0, 6);
        while (!header.empty() && header.front() == ' ')
          header.remove_prefix(1);
        while (!header.empty() && header.back() == ' ') header.remove_suffix(1);
        auto length = Number(header);
        if (!length || *length == 0 || *length > kMaxFrame) {
          Fail("invalid PMI-2 frame length");
          return;
        }
        if (c.input.size() < 6 + *length) return;
        auto request = Parse(std::string_view(c.input).substr(6, *length), ';');
        if (!request) {
          Fail("invalid PMI-2 fields");
          return;
        }
        c.input.erase(0, 6 + *length);
        Request(c, *request);
      }
    }
  }
  void IoLoop() {
    for (;;) {
      std::vector<pollfd> fds{{wake_fd, POLLIN, 0}};
      std::vector<Client*> active;
      {
        std::lock_guard lock(mutex);
        if (stopping || failed) break;
        for (auto& [rank, c] : clients) {
          if (c->fd < 0) continue;
          short events = POLLIN;
          if (!c->output.empty()) events |= POLLOUT;
          fds.push_back({c->fd, events, 0});
          active.push_back(c.get());
        }
      }
      int rc = poll(fds.data(), fds.size(), 100);
      if (rc < 0 && errno == EINTR) continue;
      std::lock_guard lock(mutex);
      if (rc < 0) {
        Fail("poll failed");
        continue;
      }
      uint64_t count;
      if (fds[0].revents & POLLIN) (void)read(wake_fd, &count, sizeof(count));
      for (size_t i = 0; i < active.size(); ++i) {
        auto& c = *active[i];
        if (c.fd < 0) continue;
        auto events = fds[i + 1].revents;
        if (events & (POLLIN | POLLHUP)) {
          char buffer[8192];
          auto bytes = recv(c.fd, buffer, sizeof(buffer), 0);
          if (bytes > 0) {
            if (c.input.empty())
              c.deadline = std::chrono::steady_clock::now() + timeout;
            c.input.append(buffer, bytes);
            Decode(c);
          } else if (bytes == 0 || (errno != EINTR && errno != EAGAIN &&
                                    errno != EWOULDBLOCK)) {
            close(std::exchange(c.fd, -1));
            if (!c.finalized) Fail("client disconnected before Finalize");
          }
        }
        if (c.fd >= 0 && !c.output.empty() && (events & POLLOUT)) {
          auto bytes =
              send(c.fd, c.output.data(), c.output.size(), MSG_NOSIGNAL);
          if (bytes > 0)
            c.output.erase(0, bytes);
          else if (bytes < 0 && errno != EINTR && errno != EAGAIN &&
                   errno != EWOULDBLOCK)
            Fail("client write failed");
        }
        if (c.fd >= 0 && c.closing && c.output.empty())
          close(std::exchange(c.fd, -1));
        if ((!c.initialized || !c.input.empty() || c.waiting) && !c.closing &&
            std::chrono::steady_clock::now() >= c.deadline)
          Fail("client request timed out");
        if (events & (POLLERR | POLLNVAL)) Fail("client channel failed");
      }
    }
    // Invoke scheduler code outside the protocol lock. It may synchronously
    // request task cleanup, which calls back into TaskExited().
    bool report;
    {
      std::lock_guard lock(mutex);
      for (auto& [rank, c] : clients)
        if (c->fd >= 0) close(std::exchange(c->fd, -1));
      report = failed && !failure_reported;
      failure_reported = true;
    }
    if (report) {
      // Propagate only along tree edges. Each server reports failure once, so
      // cancellation reaches all ranks without an all-to-all RPC storm.
      std::vector<std::shared_ptr<crane::grpc::pmi2::Pmi2::Stub>> neighbors;
      {
        std::lock_guard lock(mutex);
        for (const auto& [id, stub] : peers)
          if (children.contains(id) || parent == id) neighbors.push_back(stub);
      }
      for (const auto& stub : neighbors) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::seconds(2));
        crane::grpc::pmi2::AbortRequest request;
        crane::grpc::pmi2::AbortReply reply;
        request.set_job_id(step.job_id());
        request.set_step_id(step.step_id());
        request.set_craned_id(config.CranedIdOfThisNode);
        (void)stub->Abort(&context, request, &reply);
      }
      if (on_failure) on_failure();
    }
  }
};

Server::Server() : impl_(std::make_unique<Impl>()) {}
Server::~Server() = default;

bool Server::Init(const mpi::Config& config, const crane::grpc::StepToD& step,
                  std::function<void()> on_failure,
                  std::chrono::seconds timeout) {
  auto& s = *impl_;
  if (s.wake_fd >= 0 || timeout <= std::chrono::seconds::zero() ||
      step.task_node_list_size() == 0)
    return false;
  std::set<CranedId> unique_nodes(step.nodelist().begin(),
                                  step.nodelist().end());
  if (unique_nodes.size() != static_cast<size_t>(step.nodelist_size()))
    return false;
  auto it = std::ranges::find(step.nodelist(), config.CranedIdOfThisNode);
  if (it == step.nodelist().end()) return false;
  s.node = it - step.nodelist().begin();
  s.local_size = 0;
  std::vector<bool> active(step.nodelist_size());
  for (auto node : step.task_node_list()) {
    if (node >= step.nodelist_size()) return false;
    active[node] = true;
    if (node == s.node) ++s.local_size;
  }

  s.config = config;
  s.step = step;
  s.timeout = timeout;
  s.on_failure = std::move(on_failure);
  // Legacy Open MPI PMI-2 components parse this as a numeric job.step pair.
  s.jobid = fmt::format("{}.{}", step.job_id(), step.step_id());
  s.mapping = Mapping(step);
  if (s.mapping.size() >= kMaxValue) return false;
  s.wake_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (s.wake_fd < 0) return false;
  std::vector<CranedId> members;
  for (int index = 0; index < step.nodelist_size(); ++index)
    if (active[index]) members.push_back(step.nodelist(index));
  auto member = std::ranges::find(members, config.CranedIdOfThisNode);
  if (member != members.end()) {
    auto index = static_cast<size_t>(member - members.begin());
    if (index > 0) s.parent = members[(index - 1) / 2];
    for (auto child : {index * 2 + 1, index * 2 + 2})
      if (child < members.size()) s.children.insert(members[child]);
  }
  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);
  if (config.CompressedRpc) ServerBuilderSetCompression(&builder);
  int port = 0;
  if (config.UseTls)
    ServerBuilderAddTcpTlsListeningRandomPort(&builder, "0.0.0.0",
                                              config.TlsCerts, &port);
  else
    ServerBuilderAddTcpInsecureListeningRandomPort(&builder, "0.0.0.0", &port);
  builder.RegisterService(&s);
  s.rpc_server = builder.BuildAndStart();
  if (!s.rpc_server || port == 0) return false;
  s.port = std::to_string(port);
  s.io_thread = std::thread([&s] { s.IoLoop(); });
  if (s.local_size > 0) s.fence_thread = std::thread([&s] { s.FenceLoop(); });
  return true;
}

std::optional<mpi::LaunchInfo> Server::PrepareTask(uint32_t rank) {
  auto& s = *impl_;
  std::lock_guard lock(s.mutex);
  if (s.wake_fd < 0 || s.failed || s.stopping ||
      rank >= s.step.task_node_list_size() ||
      s.step.task_node_list(rank) != s.node || s.clients.contains(rank))
    return std::nullopt;
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0)
    return std::nullopt;
  if (fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1) {
    close(fds[0]);
    close(fds[1]);
    return std::nullopt;
  }
  auto client = std::make_unique<Impl::Client>();
  client->fd = fds[0];
  client->rank = rank;
  client->deadline = std::chrono::steady_clock::now() + s.timeout;
  s.clients.emplace(rank, std::move(client));
  mpi::LaunchInfo launch;
  launch.fd = fds[1];
  uint32_t local_rank = 0;
  for (uint32_t preceding = 0; preceding < rank; ++preceding)
    if (s.step.task_node_list(preceding) == s.node) ++local_rank;
  launch.env = {{"PMI_FD", std::to_string(launch.fd)},
                {"PMI_RANK", std::to_string(rank)},
                {"PMI_SIZE", std::to_string(s.step.task_node_list_size())},
                {"PMI_JOBID", s.jobid},
                {"PMI_LOCAL_RANK", std::to_string(local_rank)},
                {"PMI_LOCAL_SIZE", std::to_string(s.local_size)}};
  s.Wake();
  return launch;
}

bool Server::ReceivePeers(
    const std::vector<std::pair<CranedId, std::string>>& peers) {
  auto& s = *impl_;
  std::map<CranedId, std::shared_ptr<crane::grpc::pmi2::Pmi2::Stub>> stubs;
  std::map<CranedId, std::string> ports;
  for (const auto& [id, port] : peers) {
    auto number = Number(port);
    if (id == s.config.CranedIdOfThisNode || !number || *number == 0 ||
        *number > 65535 ||
        std::ranges::find(s.step.nodelist(), id) == s.step.nodelist().end() ||
        !s.config.CranedIdToNodeHostname.contains(id) ||
        !s.config.CranedIdToNodeAddr.contains(id) || stubs.contains(id))
      return false;
    grpc::ChannelArguments args;
    std::shared_ptr<grpc::Channel> channel;
    if (s.config.UseTls) {
      SetTlsTargetNameOverride(&args, s.config.CranedIdToNodeHostname.at(id));
      channel = CreateTcpTlsCustomChannelByIp(
          s.config.CranedIdToNodeAddr.at(id), port, s.config.TlsCerts, args);
    } else {
      channel = CreateTcpInsecureCustomChannel(
          s.config.CranedIdToNodeAddr.at(id), port, args);
    }
    stubs.emplace(id, crane::grpc::pmi2::Pmi2::NewStub(channel));
    ports.emplace(id, port);
  }
  if (stubs.size() + 1 != static_cast<size_t>(s.step.nodelist_size()))
    return false;
  std::lock_guard lock(s.mutex);
  if (s.failed || s.stopping) return false;
  // Endpoint delivery can be retried; never replace a stub used by an in-flight
  // RPC.
  if (!s.peer_ports.empty()) return s.peer_ports == ports;
  s.peer_ports = std::move(ports);
  s.peers = std::move(stubs);
  s.cv.notify_all();
  return true;
}
void Server::TaskExited(uint32_t rank) {
  auto& s = *impl_;
  std::lock_guard lock(s.mutex);
  auto it = s.clients.find(rank);
  if (it != s.clients.end() && !it->second->finalized)
    s.Fail("rank exited before Finalize");
}
std::string Server::Port() const { return impl_->port; }
}  // namespace pmi2
