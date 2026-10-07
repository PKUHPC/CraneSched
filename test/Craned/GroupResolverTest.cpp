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

TEST(GroupResolver, ContainerStepUsesSubmittingUserGroups) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_userns(true);
  // Container IDs do not have to identify a host NSS account.
  step.mutable_pod_meta()->set_run_as_user(42420);
  step.mutable_pod_meta()->set_run_as_group(42421);

  auto result = GroupResolver::ResolveStep(step);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(result->gids.front(), current.Gid());
  EXPECT_EQ(GroupResolver::ExecutionUid(step), current.Uid());
  EXPECT_TRUE(GroupResolver::ResolveStep(step, {current.Gid()}));
}

TEST(GroupResolver, PodUserCannotAuthorizeAnInvalidHostUser) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(std::numeric_limits<uint32_t>::max());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_userns(true);

  EXPECT_FALSE(GroupResolver::ResolveStep(step));
}

TEST(GroupResolver, RejectsUsernsSupplementaryGroupsBeforeNssReconciliation) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());
  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());
  step.add_gids(current.Gid() == 0 ? 1 : 0);
  step.mutable_pod_meta()->set_userns(true);
  auto result = GroupResolver::ResolveStep(step);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().find("supplementary groups"), std::string::npos);
}

TEST(GroupResolver, ContainerUidNeverSelectsHostNssIdentity) {
  crane::grpc::StepToD step;
  step.set_uid(1000);
  step.mutable_pod_meta()->set_run_as_user(42);
  EXPECT_EQ(GroupResolver::ExecutionUid(step), 1000);
}

TEST(GroupResolver, NativeStepUsesSubmittingUserGroups) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());

  auto result = GroupResolver::ResolveStep(step);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(result->gids.front(), current.Gid());
}

}  // namespace Craned
