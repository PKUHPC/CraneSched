/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include "Pmi1Session.h"

TEST(Pmi1Session, SingletonLifecycleKvsAndBufferContract) {
  const char* previous = getenv("PMI_FD");
  const std::optional<std::string> saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  unsetenv("PMI_FD");
  crane::pmi1::Session session;
  int spawned, initialized, size;
  EXPECT_EQ(session.Initialized(&initialized), PMI_SUCCESS);
  EXPECT_EQ(initialized, 0);
  EXPECT_EQ(session.Initialize(&spawned), PMI_SUCCESS);
  EXPECT_EQ(spawned, 0);
  EXPECT_EQ(
      session.GetInteger(crane::pmi1::Session::Integer::kCliqueSize, &size),
      PMI_SUCCESS);
  EXPECT_EQ(size, 1);
  EXPECT_EQ(session.GetInteger(crane::pmi1::Session::Integer::kAppnum, &size),
            PMI_SUCCESS);
  EXPECT_EQ(size, 0);
  char name[256], value[32];
  ASSERT_EQ(session.KvsName(name, sizeof(name)), PMI_SUCCESS);
  EXPECT_EQ(session.Put(name, "key", "value"), PMI_SUCCESS);
  EXPECT_EQ(session.Commit(name), PMI_SUCCESS);
  EXPECT_EQ(session.Barrier(), PMI_SUCCESS);
  EXPECT_EQ(session.Get(name, "key", value, 2), PMI_ERR_INVALID_LENGTH);
  EXPECT_EQ(session.Get(name, "key", value, sizeof(value)), PMI_SUCCESS);
  EXPECT_STREQ(value, "value");
  EXPECT_EQ(session.Finalize(), PMI_SUCCESS);
  EXPECT_EQ(session.Initialize(&spawned), PMI_ERR_INIT);
  if (saved) setenv("PMI_FD", saved->c_str(), 1);
}

TEST(Pmi1Session, AbortExitsAfterUnlockingTheSession) {
  EXPECT_EXIT(
      {
        unsetenv("PMI_FD");
        int spawned;
        if (PMI_Init(&spawned) != PMI_SUCCESS) _exit(20);
        atexit([] { PMI_Finalize(); });
        PMI_Abort(17, "singleton abort");
      },
      ::testing::ExitedWithCode(17), "");
}
