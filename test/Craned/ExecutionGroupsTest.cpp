#include <gtest/gtest.h>
#include <unistd.h>

#include <limits>

#include "crane/OS.h"
#include "crane/PasswordEntry.h"

namespace util::os {

TEST(ExecutionGroups, KeepsRequestedOrderAndIntersection) {
  auto result = ReconcileGroups({100, 300, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 300, 200}));
}

TEST(ExecutionGroups, DropsFrontendOnlyGroups) {
  auto result = ReconcileGroups({100, 200, 999}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, DoesNotGrantBackendOnlyGroups) {
  auto result = ReconcileGroups({100, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, DeduplicatesRequestedGroups) {
  auto result = ReconcileGroups({100, 200, 200, 100}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, RejectsMissingPrimaryGroup) {
  auto result = ReconcileGroups({100, 200}, {200, 300});
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().find("PRIMARY_GID_NOT_AUTHORIZED"),
            std::string::npos);
}

TEST(ExecutionGroups, RejectsEmptyAndOversizedRequests) {
  EXPECT_FALSE(ReconcileGroups({}, {100}).has_value());
  EXPECT_FALSE(
      ReconcileGroups(std::vector<uint32_t>(257, 100), {100}).has_value());
}

TEST(ExecutionGroups, ContainerIdentityDoesNotRequireHostNssAccount) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  // Container IDs do not have to identify a host NSS account.
  EXPECT_TRUE(ValidateContainerIdentity(
      current.Uid(), std::vector<uint32_t>{current.Gid()}, true, 42420, 42421));
  auto result = ResolveGroups(current.Uid(), {current.Gid()});
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(*result, (std::vector<gid_t>{current.Gid()}));
}

TEST(ExecutionGroups, RejectsUnknownHostUser) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());
  EXPECT_FALSE(
      ResolveGroups(std::numeric_limits<uint32_t>::max(), {current.Gid()}));
}

TEST(ExecutionGroups, UsernsRejectsGroupsThatNssWouldDiscard) {
  const std::vector<uint32_t> requested{1000, 2000};
  auto resolved = ReconcileGroups(requested, {1000});
  ASSERT_TRUE(resolved);
  EXPECT_EQ(*resolved, (std::vector<gid_t>{1000}));

  // The userns restriction applies to the original submission, even if NSS
  // reconciliation would remove its supplementary groups.
  auto valid = ValidateContainerIdentity(1000, requested, true, 0, 0);
  ASSERT_FALSE(valid);
  EXPECT_NE(valid.error().find("supplementary groups"), std::string::npos);
}

TEST(ExecutionGroups, ContainerIdentityValidationDoesNotQueryNss) {
  // Identity policy validation is independent of host account lookup and
  // of the mapping bounds that are checked later on the execution node.
  constexpr auto unknown_id = std::numeric_limits<uint32_t>::max();
  EXPECT_TRUE(ValidateContainerIdentity(unknown_id, std::vector<uint32_t>{1000},
                                        true, unknown_id, unknown_id));
}

TEST(ExecutionGroups, ResolvesSubmittingUserGroups) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  auto result = ResolveGroups(current.Uid(), {current.Gid()});
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(*result, (std::vector<gid_t>{current.Gid()}));
}

}  // namespace util::os
