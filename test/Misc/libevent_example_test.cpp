#include <evrpc.h>
#include <spdlog/fmt/bundled/core.h>

#include <random>
#include <thread>

#include "AnonymousPipe.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "event2/thread.h"
#include "event2/util.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

static void log_cb(int severity, const char *msg) {
  switch (severity) {
    case _EVENT_LOG_DEBUG:
      SPDLOG_DEBUG("{}", msg);
      break;
    case _EVENT_LOG_MSG:
      SPDLOG_INFO("{}", msg);
      break;
    case _EVENT_LOG_WARN:
      SPDLOG_WARN("{}", msg);
      break;
    case _EVENT_LOG_ERR:
      SPDLOG_ERROR("{}", msg);
      break;
    default:
      break; /* never reached */
  }
}

class STATIC_FUNC {
  STATIC_FUNC() {
    event_enable_debug_mode();
    event_enable_debug_logging(EVENT_DBG_ALL);
    event_set_log_callback(log_cb);
  }
};

static STATIC_FUNC static_func_();

// NOTICE: The test result indicates that this handler
// must be installed before any fork().
static void sigchld_handler(int sig) {
  pid_t pid;
  int child_status;
  pid = wait(&child_status);
  if (pid < 0) {
    SPDLOG_ERROR("SIGCHLD received too early. Error:{}", strerror(errno));
  } else
    SPDLOG_INFO("SIGCHLD received too early. PID: {}", pid);
}

struct LibEventTest : testing::Test {
  LibEventTest()
      : m_ev_sigchld_(nullptr), m_ev_base_(nullptr), testing::Test() {
    evthread_use_pthreads();
  }

  void SetUp() override {
    signal(SIGCHLD, sigchld_handler);
    m_ev_base_ = event_base_new();
    if (!m_ev_base_) {
      FAIL() << "Could not initialize libevent!";
    }
  }

  void TearDown() override {
    if (m_ev_base_) event_base_free(m_ev_base_);
  }

  static std::string RandomFileNameStr() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist(100000, 999999);

    return std::to_string(dist(mt));
  }

  static void read_uv_stream(struct bufferevent *bev, void *user_data) {
    auto *this_ = reinterpret_cast<LibEventTest *>(user_data);

    size_t buf_len = evbuffer_get_length(bev->input);
    SPDLOG_INFO("Trying to read buffer(len: {})...", buf_len);
    EXPECT_GT(buf_len, 0);

    char str_[buf_len + 1];
    int n_copy = evbuffer_remove(bev->input, str_, buf_len);
    str_[buf_len] = '\0';
    std::string_view str(str_);

    SPDLOG_INFO("Read from child(Copied: {} bytes): {}", n_copy, str);
    EXPECT_EQ(str, this_->m_expected_str_);
  }

  static void sigchld_handler_func(evutil_socket_t sig, short events,
                                   void *user_data) {
    auto *this_ = reinterpret_cast<LibEventTest *>(user_data);

    SPDLOG_INFO("SIGCHLD received...");
    int status;
    wait(&status);

    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 1);

    timeval delay{0, 0};
    event_base_loopexit(this_->m_ev_base_, &delay);
  }

  struct event_base *m_ev_base_;
  event *m_ev_sigchld_;

  std::string m_expected_str_;
};

TEST_F(LibEventTest, IoRedirectAndDynamicTaskAdding) {
  std::string test_prog_path = "/tmp/craned_test_" + RandomFileNameStr();
  std::string prog_text =
      "#include <iostream>\\n"
      "#include <thread>\\n"
      "#include <chrono>\\n"
      "int main() { std::cout<<\"Hello World!\";"
      //"std::this_thread::sleep_for(std::chrono::seconds(1));"
      "return 1;"
      "}";

  m_expected_str_ = "Hello World!";

  std::string cmd;

  cmd = fmt::format(R"(bash -c 'echo -e '"'"'{}'"'" | g++ -xc++ -o {} -)",
                    prog_text, test_prog_path);
  SPDLOG_TRACE("Cmd: {}", cmd);
  system(cmd.c_str());

  std::vector<const char *> args{test_prog_path.c_str(), nullptr};

  u_char val{};
  bool _;
  AnonymousPipe anon_pipe;

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child
    anon_pipe.CloseParentEnd();

    dup2(anon_pipe.GetChildEndFd(), 1);  // stdout -> pipe
    dup2(anon_pipe.GetChildEndFd(), 2);  // stderr -> pipe

    _ = anon_pipe.ReadIntegerFromParent<u_char>(&val);
    anon_pipe.CloseChildEnd();  // This descriptor is no longer needed.

    std::vector<const char *> argv{test_prog_path.c_str(), nullptr};

    execvp(test_prog_path.c_str(), const_cast<char *const *>(argv.data()));
  } else {  // Parent
    anon_pipe.CloseChildEnd();

    struct bufferevent *ev_buf_event;
    ev_buf_event =
        bufferevent_socket_new(m_ev_base_, anon_pipe.GetParentEndFd(), 0);
    if (!ev_buf_event) {
      FAIL() << "Error constructing bufferevent!";
    }
    bufferevent_setcb(ev_buf_event, read_uv_stream, nullptr, nullptr, this);
    bufferevent_enable(ev_buf_event, EV_READ);
    bufferevent_disable(ev_buf_event, EV_WRITE);

    _ = anon_pipe.WriteIntegerToChild<u_char>(val);

    // Persistent event. No need to reactivate it.
    this->m_ev_sigchld_ =
        evsignal_new(this->m_ev_base_, SIGCHLD, sigchld_handler_func, this);
    if (!this->m_ev_sigchld_) {
      FAIL() << "Could not create a signal event!";
    }

    if (event_add(this->m_ev_sigchld_, nullptr) < 0) {
      FAIL() << "Could not add a signal event!";
    }

    event_base_dispatch(m_ev_base_);
    bufferevent_free(ev_buf_event);
    if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  }
  if (remove(test_prog_path.c_str()) != 0)
    SPDLOG_ERROR("Error removing test_prog:", strerror(errno));
}

static void CustomEventCb(int sock, short which, void *arg) {
  static int times = 0;
  std::this_thread::sleep_for(std::chrono::seconds(3));
  CRANE_INFO("CustomEventCb Called");
  times++;
  if (times == 2) {
    timeval val{0, 0};
    event_base_loopexit(reinterpret_cast<event_base *>(arg), &val);
  }
}

// Just a code example.
TEST_F(LibEventTest, CustomEvent) {
  struct event *ev = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                               CustomEventCb, m_ev_base_);
  event_add(ev, nullptr);

  std::thread t([=] {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    event_active(ev, 0, 0);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    event_active(ev, 0, 0);
  });

  event_base_dispatch(m_ev_base_);

  CRANE_INFO("Loop Exit");

  t.join();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  event_free(ev);
}

static void onTimer(int, short, void *arg) { CRANE_INFO("onTimer!"); }

TEST_F(LibEventTest, TimerEvent) {
  struct event *timer_event;
  timer_event = event_new(m_ev_base_, -1, 0, onTimer, nullptr);
  struct timeval tv {};

  tv.tv_sec = 2;

  evtimer_add(timer_event, &tv);

  event_base_dispatch(m_ev_base_);
  CRANE_INFO("Loop Exit");

  event_free(timer_event);
}