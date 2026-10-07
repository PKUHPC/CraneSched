#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <gtest/gtest.h>
#include <unistd.h>

#include <limits>

#include "CommonPublicDefs.h"
#include "crane/PasswordEntry.h"

namespace Craned::Common {

TEST(ExecutionGroups, KeepsRequestedOrderAndIntersection) {
  auto result = util::os::ReconcileGroups({100, 300, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 300, 200}));
}

TEST(ExecutionGroups, DropsFrontendOnlyGroups) {
  auto result = util::os::ReconcileGroups({100, 200, 999}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, DoesNotGrantBackendOnlyGroups) {
  auto result = util::os::ReconcileGroups({100, 200}, {100, 200, 300});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, DeduplicatesRequestedGroups) {
  auto result = util::os::ReconcileGroups({100, 200, 200, 100}, {100, 200});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, (std::vector<gid_t>{100, 200}));
}

TEST(ExecutionGroups, RejectsMissingPrimaryGroup) {
  auto result = util::os::ReconcileGroups({100, 200}, {200, 300});
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().find("PRIMARY_GID_NOT_AUTHORIZED"),
            std::string::npos);
}

TEST(ExecutionGroups, RejectsEmptyAndOversizedRequests) {
  EXPECT_FALSE(util::os::ReconcileGroups({}, {100}).has_value());
  EXPECT_FALSE(util::os::ReconcileGroups(std::vector<uint32_t>(257, 100), {100})
                   .has_value());
}

TEST(ExecutionGroups, ContainerStepUsesSubmittingUserGroups) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_userns(true);
  // Container IDs do not have to identify a host NSS account.
  step.mutable_pod_meta()->set_run_as_user(42420);
  step.mutable_pod_meta()->set_run_as_group(42421);

  auto result = ResolveStepGroups(step);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(result->front(), current.Gid());
  EXPECT_TRUE(ResolveStepGroups(step, {current.Gid()}));
}

TEST(ExecutionGroups, PodUserCannotAuthorizeAnInvalidHostUser) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(std::numeric_limits<uint32_t>::max());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_userns(true);

  EXPECT_FALSE(ResolveStepGroups(step));
}

TEST(ExecutionGroups, RejectsUsernsSupplementaryGroupsBeforeNssReconciliation) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());
  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());
  step.add_gids(current.Gid() == 0 ? 1 : 0);
  step.mutable_pod_meta()->set_userns(true);
  auto result = ResolveStepGroups(step);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().find("supplementary groups"), std::string::npos);
}

TEST(ExecutionGroups, ContainerUidNeverSelectsHostNssIdentity) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());
  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());
  step.mutable_pod_meta()->set_userns(true);
  step.mutable_pod_meta()->set_run_as_user(
      std::numeric_limits<uint32_t>::max());
  auto result = ResolveStepGroups(step);
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(result->front(), current.Gid());
}

TEST(ExecutionGroups, NativeStepUsesSubmittingUserGroups) {
  PasswordEntry current(getuid());
  ASSERT_TRUE(current.Valid());

  crane::grpc::StepToD step;
  step.set_uid(current.Uid());
  step.add_gids(current.Gid());

  auto result = ResolveStepGroups(step);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(result->front(), current.Gid());
}

}  // namespace Craned::Common
