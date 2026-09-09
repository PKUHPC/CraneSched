#include <gtest/gtest.h>
#include <unistd.h>

#include <limits>

#include "GroupResolver.h"
#include "crane/PasswordEntry.h"

namespace Craned {

TEST(GroupResolver, KeepsRequestedOrderAndIntersection) {
  auto result = GroupResolver::Reconcile({100, 300, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gids, (std::vector<gid_t>{100, 300, 200}));
  EXPECT_FALSE(result->diagnostics.HasMismatch());
}

TEST(GroupResolver, DropsFrontendOnlyGroups) {
  auto result = GroupResolver::Reconcile({100, 200, 999}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gids, (std::vector<gid_t>{100, 200}));
  EXPECT_EQ(result->diagnostics.dropped_supplementary_count, 1);
  EXPECT_TRUE(result->diagnostics.HasMismatch());
}

TEST(GroupResolver, DoesNotGrantBackendOnlyGroups) {
  auto result = GroupResolver::Reconcile({100, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gids, (std::vector<gid_t>{100, 200}));
  EXPECT_EQ(result->diagnostics.backend_only_count, 1);
  EXPECT_TRUE(result->diagnostics.HasMismatch());
}

TEST(GroupResolver, DeduplicatesRequestedGroupsWithoutFalseMismatch) {
  auto result = GroupResolver::Reconcile({100, 200, 200, 100}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gids, (std::vector<gid_t>{100, 200}));
  EXPECT_EQ(result->diagnostics.requested_supplementary_count, 1);
  EXPECT_EQ(result->diagnostics.accepted_supplementary_count, 1);
  EXPECT_EQ(result->diagnostics.dropped_supplementary_count, 0);
  EXPECT_FALSE(result->diagnostics.HasMismatch());
}

TEST(GroupResolver, RejectsMissingPrimaryGroup) {
  auto result = GroupResolver::Reconcile({100, 200}, {200, 300});
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().find("PRIMARY_GID_NOT_AUTHORIZED"),
            std::string::npos);
}

TEST(GroupResolver, RejectsEmptyAndOversizedRequests) {
  EXPECT_FALSE(GroupResolver::Reconcile({}, {100}).has_value());
  EXPECT_FALSE(GroupResolver::Reconcile(std::vector<uint32_t>(257, 100), {100})
                   .has_value());
}

TEST(GroupResolver, ContainerStepUsesPodRunAsUser) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(std::numeric_limits<uint32_t>::max());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_run_as_user(current.Uid());

  auto result = GroupResolver::ResolveStep(step);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(result->gids.front(), current.Gid());
}

}  // namespace Craned
