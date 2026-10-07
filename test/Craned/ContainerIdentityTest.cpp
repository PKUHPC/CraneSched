#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <gtest/gtest.h>

#include <array>

// Include the implementation to exercise its private CRI configuration helper
// without exposing a production interface solely for tests.
#include "../../src/Craned/Supervisor/TaskManager.cpp"

namespace Craned::Supervisor {

template <typename SecurityContext>
class ContainerIdentityTest : public ::testing::Test {};

using SecurityContexts =
    ::testing::Types<runtime::v1::LinuxSandboxSecurityContext,
                     runtime::v1::LinuxContainerSecurityContext>;
TYPED_TEST_SUITE(ContainerIdentityTest, SecurityContexts);

template <typename SecurityContext>
void AddMappings(SecurityContext* ctx) {
  auto* ns = ctx->mutable_namespace_options()->mutable_userns_options();
  ns->set_mode(runtime::v1::POD);
  auto* uid = ns->add_uids();
  uid->set_container_id(0);
  uid->set_host_id(100000);
  uid->set_length(65536);
  auto* gid = ns->add_gids();
  gid->set_container_id(0);
  gid->set_host_id(200000);
  gid->set_length(32768);
}

TYPED_TEST(ContainerIdentityTest, KeepsContainerRootSeparateFromHostIdentity) {
  TypeParam ctx;
  AddMappings(&ctx);
  ctx.add_supplemental_groups(1600);
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  pod.set_run_as_user(0);
  pod.set_run_as_group(0);

  auto result =
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx);
  ASSERT_TRUE(result.has_value()) << result.error();
  EXPECT_EQ(ctx.run_as_user().value(), 0);
  EXPECT_EQ(ctx.run_as_group().value(), 0);
  EXPECT_EQ(ctx.supplemental_groups_size(), 0);
}

TYPED_TEST(ContainerIdentityTest, PreservesNonzeroUsernsIdentityAndMappings) {
  TypeParam ctx;
  AddMappings(&ctx);
  const auto original = ctx.namespace_options().SerializeAsString();
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  pod.set_run_as_user(123);
  pod.set_run_as_group(456);
  auto result =
      SetContainerIdentity_(1000, std::array<gid_t, 1>{2000}, pod, &ctx);
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(ctx.run_as_user().value(), 123);
  EXPECT_EQ(ctx.run_as_group().value(), 456);
  EXPECT_EQ(ctx.supplemental_groups_size(), 0);
  EXPECT_EQ(ctx.supplemental_groups_policy(), runtime::v1::Strict);
  EXPECT_EQ(ctx.namespace_options().SerializeAsString(), original);
  EXPECT_TRUE(SetContainerIdentity_(0, std::array<gid_t, 1>{0}, pod, &ctx));
}

TYPED_TEST(ContainerIdentityTest, ChecksUidAndGidAgainstTheirOwnRanges) {
  TypeParam ctx;
  AddMappings(&ctx);
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  pod.set_run_as_user(65535);
  pod.set_run_as_group(32767);
  EXPECT_TRUE(
      SetContainerIdentity_(1000, std::array<gid_t, 1>{2000}, pod, &ctx));
  pod.set_run_as_user(65536);
  auto result =
      SetContainerIdentity_(1000, std::array<gid_t, 1>{2000}, pod, &ctx);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().find("UID is outside"), std::string::npos);
  pod.set_run_as_user(123);
  pod.set_run_as_group(32768);
  result = SetContainerIdentity_(1000, std::array<gid_t, 1>{2000}, pod, &ctx);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().find("GID is outside"), std::string::npos);
  // A root submitter must obey the same mapping bounds.
  EXPECT_FALSE(SetContainerIdentity_(0, std::array<gid_t, 1>{0}, pod, &ctx));
}

TYPED_TEST(ContainerIdentityTest, RejectsUsernsSupplementaryGroups) {
  TypeParam ctx;
  AddMappings(&ctx);
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  pod.set_run_as_user(123);
  pod.set_run_as_group(456);
  auto result = SetContainerIdentity_(
      1000, std::array<gid_t, 3>{1000, 2000, 2001}, pod, &ctx);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().find("supplementary groups"), std::string::npos);
  EXPECT_TRUE(
      SetContainerIdentity_(1000, std::array<gid_t, 2>{2000, 2000}, pod, &ctx));
  EXPECT_EQ(ctx.run_as_group().value(), 456);
  EXPECT_EQ(ctx.supplemental_groups_size(), 0);
  EXPECT_EQ(ctx.supplemental_groups_policy(), runtime::v1::Strict);
}

TYPED_TEST(ContainerIdentityTest,
           PreservesHostEffectiveAndSupplementaryGroups) {
  TypeParam ctx;
  ctx.add_supplemental_groups(9999);
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_run_as_user(1000);
  pod.set_run_as_group(2000);
  auto result = SetContainerIdentity_(
      1000, std::array<gid_t, 3>{2000, 1000, 2001}, pod, &ctx);
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(ctx.run_as_user().value(), 1000);
  EXPECT_EQ(ctx.run_as_group().value(), 2000);
  ASSERT_EQ(ctx.supplemental_groups_size(), 2);
  EXPECT_EQ(ctx.supplemental_groups(0), 1000);
  EXPECT_EQ(ctx.supplemental_groups(1), 2001);
  EXPECT_EQ(ctx.supplemental_groups_policy(), runtime::v1::Strict);
}

TYPED_TEST(ContainerIdentityTest, RejectsEmptyHostGroups) {
  TypeParam ctx;
  crane::grpc::PodJobAdditionalMeta pod;
  EXPECT_FALSE(SetContainerIdentity_(1000, {}, pod, &ctx));
  pod.set_userns(true);
  AddMappings(&ctx);
  EXPECT_FALSE(SetContainerIdentity_(1000, {}, pod, &ctx));
}

TYPED_TEST(ContainerIdentityTest, RequiresRootInBothMappings) {
  TypeParam ctx;
  AddMappings(&ctx);
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  auto* mappings = ctx.mutable_namespace_options()->mutable_userns_options();
  mappings->mutable_uids(0)->set_container_id(1);
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx));
  mappings->mutable_uids(0)->set_container_id(0);
  mappings->mutable_gids(0)->set_container_id(1);
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx));
  // Root must also stay within the user namespace's mapping.
  EXPECT_FALSE(SetContainerIdentity_(0, std::array<gid_t, 1>{0}, pod, &ctx));
}

TYPED_TEST(ContainerIdentityTest, RejectsMissingMapping) {
  TypeParam ctx;
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_userns(true);
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx));
  AddMappings(&ctx);
  ctx.mutable_namespace_options()->mutable_userns_options()->clear_gids();
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx));
}

TYPED_TEST(ContainerIdentityTest, PreservesHostNamespaceRestrictions) {
  TypeParam ctx;
  crane::grpc::PodJobAdditionalMeta pod;
  pod.set_run_as_user(1500);
  pod.set_run_as_group(1600);
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1500}, pod, &ctx));
  // A validated effective group need not be the NSS primary group.
  EXPECT_TRUE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1600}, pod, &ctx));
  pod.set_run_as_user(0);
  EXPECT_FALSE(
      SetContainerIdentity_(1500, std::array<gid_t, 1>{1600}, pod, &ctx));
  EXPECT_FALSE(SetContainerIdentity_(0, std::array<gid_t, 1>{0}, pod, &ctx));
}

}  // namespace Craned::Supervisor
