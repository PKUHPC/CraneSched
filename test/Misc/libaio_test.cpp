/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <libaio.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <string>

std::string path = "/tmp/testfile";           // Path to the file to manipulate
constexpr int32_t file_size = 1000;           // Length of file in 4k blocks
constexpr int32_t concurrent_requests = 100;  // Number of concurrent requests
constexpr int32_t min_nr = 1;
constexpr int32_t max_nr = 1;

// The size of operation that will occur on the device
static const int kPageSize = 4096;

class AIORequest {
 public:
  int* buffer_;

  virtual void Complete(int res) = 0;

  AIORequest() { AllocBuffer(); }

  void AllocBuffer() {
    int ret = posix_memalign(reinterpret_cast<void**>(&buffer_), kPageSize,
                             kPageSize);
    ASSERT_EQ(ret, 0);
  }

  virtual ~AIORequest() { free(buffer_); }
};

class Adder {
 public:
  virtual void Add(int amount) = 0;

  virtual ~Adder(){};
};

class AIOReadRequest : public AIORequest {
 private:
  Adder* adder_;

 public:
  AIOReadRequest(Adder* adder) : AIORequest(), adder_(adder) {}

  virtual void Complete(int res) {
    ASSERT_EQ(res, kPageSize) << "Read incomplete or error " << res;
    int value = buffer_[0];
    GTEST_LOG_(INFO) << "Read of " << value << " completed";
    adder_->Add(value);
  }
};

class AIOWriteRequest : public AIORequest {
 private:
  int value_;

 public:
  AIOWriteRequest(int value) : AIORequest(), value_(value) {
    buffer_[0] = value;
  }

  virtual void Complete(int res) {
    ASSERT_EQ(res, kPageSize) << "Write incomplete or error " << res;
    GTEST_LOG_(INFO) << "Write of " << value_ << " completed";
  }
};

class AIOAdder : public Adder {
 public:
  int fd_;
  io_context_t ioctx_;
  int counter_;
  int reap_counter_;
  int sum_;
  int length_;

  AIOAdder(int length)
      : ioctx_(0), counter_(0), reap_counter_(0), sum_(0), length_(length) {}

  void Init() {
    GTEST_LOG_(INFO) << "Opening file";
    fd_ = open(path.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0644);
    ASSERT_TRUE(fd_ >= 0) << "Error opening file";
    GTEST_LOG_(INFO) << "Allocating enough space for the sum";
    ASSERT_TRUE(fallocate(fd_, 0, 0, kPageSize * length_) >= 0)
        << "Error in fallocate";
    GTEST_LOG_(INFO) << "Setting up the io context";
    ASSERT_TRUE(io_setup(100, &ioctx_) >= 0) << "Error in io_setup";
  }

  virtual void Add(int amount) {
    sum_ += amount;
    GTEST_LOG_(INFO) << "Adding " << amount << " for a total of " << sum_;
  }

  void SubmitWrite() {
    GTEST_LOG_(INFO) << "Submitting a write to " << counter_;
    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    AIORequest* req = new AIOWriteRequest(counter_);
    io_prep_pwrite(&iocb, fd_, req->buffer_, kPageSize, counter_ * kPageSize);
    iocb.data = req;
    int res = io_submit(ioctx_, 1, &iocbs);
    ASSERT_EQ(res, 1);
  }

  void WriteFile() {
    reap_counter_ = 0;
    for (counter_ = 0; counter_ < length_; counter_++) {
      SubmitWrite();
      Reap();
    }
    ReapRemaining();
  }

  void SubmitRead() {
    GTEST_LOG_(INFO) << "Submitting a read from " << counter_;
    struct iocb iocb;
    struct iocb* iocbs = &iocb;
    AIORequest* req = new AIOReadRequest(this);
    io_prep_pread(&iocb, fd_, req->buffer_, kPageSize, counter_ * kPageSize);
    iocb.data = req;
    int res = io_submit(ioctx_, 1, &iocbs);
    ASSERT_EQ(res, 1);
  }

  void ReadFile() {
    reap_counter_ = 0;
    for (counter_ = 0; counter_ < length_; counter_++) {
      SubmitRead();
      Reap();
    }
    ReapRemaining();
  }

  int DoReap(int min_nr) {
    GTEST_LOG_(INFO) << "Reaping between " << min_nr << " and " << max_nr
                     << " io_events";
    struct io_event* events = new io_event[max_nr];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 100000000;
    int num_events;
    GTEST_LOG_(INFO) << "Calling io_getevents";
    num_events = io_getevents(ioctx_, min_nr, max_nr, events, &timeout);
    GTEST_LOG_(INFO) << "Calling completion function on results";
    for (int i = 0; i < num_events; i++) {
      struct io_event event = events[i];
      AIORequest* req = static_cast<AIORequest*>(event.data);
      req->Complete(event.res);
      delete req;
    }
    delete[] events;

    GTEST_LOG_(INFO) << "Reaped " << num_events << " io_events";
    reap_counter_ += num_events;
    return num_events;
  }

  void Reap() {
    if (counter_ >= min_nr) {
      DoReap(min_nr);
    }
  }

  void ReapRemaining() {
    while (reap_counter_ < length_) {
      DoReap(1);
    }
  }

  ~AIOAdder() {
    GTEST_LOG_(INFO) << "Closing AIO context and file";
    io_destroy(ioctx_);
    close(fd_);
  }

  int Sum() {
    GTEST_LOG_(INFO) << "Writing consecutive integers to file";
    WriteFile();
    GTEST_LOG_(INFO) << "Reading consecutive integers from file";
    ReadFile();
    return sum_;
  }
};

TEST(LibAIO, Simple) {
  AIOAdder adder(file_size);
  adder.Init();
  int sum = adder.Sum();
  int expected = (file_size * (file_size - 1)) / 2;
  GTEST_LOG_(INFO) << "AIO is complete";
  ASSERT_EQ(sum, expected) << "Expected " << expected << " Got " << sum;
  printf(
      "Successfully calculated that the sum of integers from 0 to %d is %d\n",
      file_size - 1, sum);
}