// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <grp.h>
#include <gtest/gtest.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <array>
#include <fstream>
#include <utility>

// Include the implementation to exercise the actual CRI configuration code
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

class ContainerMountIdentityTest
    : public ::testing::TestWithParam<std::pair<uid_t, gid_t>> {
 protected:
  static CraneErrCode ApplyMountMappings(const PasswordEntry& pwd, gid_t egid,
                                         cri::api::ContainerConfig* config) {
    ContainerInstance container(nullptr, 0);
    return container.ApplyIdMappedMounts_(pwd, egid, config, false);
  }
};

TEST_P(ContainerMountIdentityTest, MapsHostOwnershipToContainerIdentity) {
  PasswordEntry pwd(getuid());
  ASSERT_TRUE(pwd.Valid());
  // Model newgrp: the effective group differs from the NSS primary group.
  const gid_t egid = pwd.Gid() == 2000 ? 2001 : 2000;
  const auto [run_as_uid, run_as_gid] = GetParam();
  cri::api::ContainerConfig config;
  auto* ctx = config.mutable_linux()->mutable_security_context();
  AddMappings(ctx);
  ctx->mutable_run_as_user()->set_value(run_as_uid);
  ctx->mutable_run_as_group()->set_value(run_as_gid);

  auto* input = config.add_mounts();
  input->set_host_path("/host/input");
  input->set_container_path("/input");
  input->set_readonly(true);
  input->set_propagation(cri::api::PROPAGATION_PRIVATE);
  auto* output = config.add_mounts();
  output->set_host_path("/host/output");
  output->set_container_path("/output");
  output->set_propagation(cri::api::PROPAGATION_BIDIRECTIONAL);
  const auto original_config = config.SerializeAsString();

  ASSERT_EQ(ApplyMountMappings(pwd, egid, &config), CraneErrCode::SUCCESS);
  ASSERT_EQ(config.mounts_size(), 2);
  for (const auto& mount : config.mounts()) {
    SCOPED_TRACE(mount.container_path());
    ASSERT_EQ(mount.uidmappings_size(), 1);
    ASSERT_EQ(mount.gidmappings_size(), 1);
    // CRI mount container_id holds the source file's host ownership;
    // host_id holds the kernel ID visible as runAs in the user namespace.
    EXPECT_EQ(mount.uidmappings(0).container_id(), pwd.Uid());
    EXPECT_EQ(mount.uidmappings(0).host_id(), 100000 + run_as_uid);
    EXPECT_EQ(mount.uidmappings(0).length(), 1);
    EXPECT_EQ(mount.gidmappings(0).container_id(), egid);
    EXPECT_EQ(mount.gidmappings(0).host_id(), 200000 + run_as_gid);
    EXPECT_EQ(mount.gidmappings(0).length(), 1);
  }

  // Only mount mappings change; namespace mappings, runAs, paths and flags
  // must remain intact.
  for (auto& mount : *config.mutable_mounts()) {
    mount.clear_uidmappings();
    mount.clear_gidmappings();
  }
  EXPECT_EQ(config.SerializeAsString(), original_config);
}

INSTANTIATE_TEST_SUITE_P(
    Userns, ContainerMountIdentityTest,
    ::testing::Values(std::pair<uid_t, gid_t>{0, 0},
                      std::pair<uid_t, gid_t>{123, 456}),
    [](const ::testing::TestParamInfo<std::pair<uid_t, gid_t>>& info) {
      return info.param.first == 0 ? "Root" : "ExplicitUser";
    });

class CforedExitTest : public testing::Test {
 protected:
  void SetUp() override {
    client.m_stopped_ = true;
    client.m_ev_thread_.join();
    client.m_fwd_meta_map.try_emplace(0);
  }

  void Eof(bool stdout_stream) { client.HandleOutputStop_(0, stdout_stream); }
  bool Finish() { return client.FinishTaskIO_(0); }
  std::vector<uint32_t> ExitCodes() {
    std::vector<uint32_t> codes;
    CforedClient::FwdRequest req{.data = CforedClient::TaskFinishStatus{}};
    while (client.m_task_fwd_req_queue_.try_dequeue(req))
      codes.push_back(
          std::get<CforedClient::TaskFinishStatus>(req.data).exit_code);
    return codes;
  }
  void TearDown() override { client.m_fwd_meta_map.clear(); }

  CforedClient client;
};

TEST_F(CforedExitTest, FinalEofRacingWithProcessExitPublishesOnce) {
  Eof(true);
  Eof(false);
  client.TaskProcessStop(0, 7, false);
  EXPECT_TRUE(ExitCodes().empty());
  EXPECT_TRUE(Finish());
  EXPECT_FALSE(Finish());
  EXPECT_EQ(ExitCodes(), std::vector<uint32_t>{7});
}

namespace {

class TaskEnvironmentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_node_ = g_config.CranedIdOfThisNode;
    original_env_ = g_config.JobEnv;
    original_slurm_ = g_config.EnableSlurmCompatibleEnv;
    auto hostname = util::os::GetHostname();
    ASSERT_TRUE(hostname);
    g_config.CranedIdOfThisNode = hostname.value() + "-scheduler-alias";
    g_config.JobEnv = {};
    g_config.EnableSlurmCompatibleEnv = false;

    spec_.set_type(crane::grpc::Batch);
    spec_.set_step_type(crane::grpc::PRIMARY);
    spec_.add_nodelist(g_config.CranedIdOfThisNode);
    spec_.add_task_node_list(0);
    (*spec_.mutable_env())["HOSTNAME"] = "submission-host";
    (*spec_.mutable_env())["ENVIRONMENT"] = "submission-environment";
  }

  void TearDown() override {
    g_config.CranedIdOfThisNode = std::move(original_node_);
    g_config.JobEnv = std::move(original_env_);
    g_config.EnableSlurmCompatibleEnv = original_slurm_;
  }

  StepToSupv spec_;

 private:
  std::string original_node_;
  EnvMap original_env_;
  bool original_slurm_{};
};

TEST_F(TaskEnvironmentTest, BatchHostnameUsesTheOperatingSystemName) {
  auto hostname = util::os::GetHostname();
  ASSERT_TRUE(hostname);
  ASSERT_NE(hostname.value(), g_config.CranedIdOfThisNode);
  for (bool pod : {false, true}) {
    spec_.set_type(pod ? crane::grpc::Container : crane::grpc::Batch);
    if (pod) spec_.mutable_batch_meta();
    StepInstance step(spec_);
    const auto env = step.GetStepProcessEnv();
    EXPECT_EQ(env.at("HOSTNAME"), hostname.value());
    EXPECT_EQ(env.at("ENVIRONMENT"), "BATCH");
  }
}

TEST_F(TaskEnvironmentTest, OtherStepsPreserveSubmittedHostname) {
  for (auto type :
       {crane::grpc::DAEMON, crane::grpc::PRIMARY, crane::grpc::COMMON}) {
    spec_.set_step_type(type);
    spec_.set_type(crane::grpc::Interactive);
    spec_.mutable_interactive_meta()->set_interactive_type(
        type == crane::grpc::PRIMARY ? crane::grpc::Calloc : crane::grpc::Crun);
    StepInstance step(spec_);
    const auto env = step.GetStepProcessEnv();
    EXPECT_EQ(env.at("HOSTNAME"), "submission-host");
    EXPECT_EQ(env.at("ENVIRONMENT"), "submission-environment");
    step.GetMutableStep().mutable_env()->clear();
    const auto empty_env = step.GetStepProcessEnv();
    EXPECT_FALSE(empty_env.contains("HOSTNAME"));
    EXPECT_FALSE(empty_env.contains("ENVIRONMENT"));
  }
}

class ScriptPermissionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    old_directory_ = g_config.CraneScriptDir;
    old_job_ = g_config.JobId;
    old_step_ = g_config.StepId;
    old_umask_ = umask(0);
    std::string pattern = "/tmp/crane-script-permissions-XXXXXX";
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    directory_ = pattern;
    ASSERT_TRUE(util::os::EnsureServiceDirectory(directory_, 0755));
    g_config.CraneScriptDir = directory_;
    g_config.JobId = 42;
    g_config.StepId = 0;
    path_ = directory_ / "Crane-42.0.sh";
    spec_.set_type(crane::grpc::Batch);
    spec_.set_step_type(crane::grpc::PRIMARY);
    spec_.set_uid(geteuid());
    spec_.add_gids(getegid());
    spec_.set_sh_script("#!/bin/sh\nexit 0\n");
  }

  void TearDown() override {
    g_config.CraneScriptDir = std::move(old_directory_);
    g_config.JobId = old_job_;
    g_config.StepId = old_step_;
    umask(old_umask_);
    if (!directory_.empty()) std::filesystem::remove_all(directory_);
  }

  void CheckScript() {
    struct stat st{};
    ASSERT_EQ(stat(path_.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 07777, 0700);
    EXPECT_EQ(st.st_uid, spec_.uid());
    EXPECT_EQ(st.st_gid, spec_.gids(0));
    std::ifstream input(path_);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}),
              spec_.sh_script());
    EXPECT_EQ(umask(0), 0);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(directory_),
                            std::filesystem::directory_iterator{}),
              1);
  }

  StepToSupv spec_;
  std::filesystem::path directory_, path_;

 private:
  std::filesystem::path old_directory_;
  job_id_t old_job_{};
  step_id_t old_step_{};
  mode_t old_umask_{};
};

TEST_F(ScriptPermissionsTest,
       PrepareCreatesPrivateExecutableAndCleanupRemovesIt) {
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  ASSERT_EQ(step.script_path, path_);
  CheckScript();
  step.CleanUp();
  EXPECT_FALSE(std::filesystem::exists(path_));
}

TEST_F(ScriptPermissionsTest, ReplacesExistingScriptWithoutReusingItsInode) {
  std::ofstream(path_) << "old content";
  ASSERT_EQ(chmod(path_.c_str(), 0755), 0);
  struct stat before{};
  ASSERT_EQ(stat(path_.c_str(), &before), 0);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  CheckScript();
  struct stat after{};
  ASSERT_EQ(stat(path_.c_str(), &after), 0);
  EXPECT_NE(before.st_ino, after.st_ino);
}

TEST_F(ScriptPermissionsTest, FailedPreparationRemovesUnpublishedFile) {
  spec_.clear_gids();
  StepInstance step(spec_);
  EXPECT_EQ(step.Prepare(), CraneErrCode::ERR_SYSTEM_ERR);
  EXPECT_FALSE(step.script_path.has_value());
  EXPECT_TRUE(std::filesystem::is_empty(directory_));
}

TEST_F(ScriptPermissionsTest, ScriptExecutesAsTheJobUser) {
  if (geteuid() != 0) GTEST_SKIP() << "Requires root to test another UID";
  spec_.set_uid(65534);
  spec_.set_gids(0, 65534);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  CheckScript();
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setgid(65534) != 0 || setuid(65534) != 0)
      _exit(2);
    execl("/bin/bash", "bash", "-c", path_.c_str(), nullptr);
    _exit(3);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(ScriptPermissionsTest, OtherUserInJobGroupCannotReadScript) {
  if (geteuid() != 0) GTEST_SKIP() << "Requires root to test another UID";
  spec_.set_uid(65534);
  spec_.set_gids(0, 65534);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setgid(65534) != 0 || setuid(65533) != 0)
      _exit(2);
    int fd = open(path_.c_str(), O_RDONLY);
    _exit(fd == -1 && errno == EACCES ? 0 : 1);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}
class ResidualTask : public ITaskInstance {
 public:
  using ITaskInstance::ITaskInstance;
  std::optional<TaskExecId> GetExecId() const override { return pid_t{123}; }
  CraneErrCode Kill(int sig) override {
    signals.push_back(sig);
    return result;
  }
  CraneErrCode Spawn() override { return CraneErrCode::SUCCESS; }
  CraneErrCode Prepare() override { return CraneErrCode::SUCCESS; }
  CraneErrCode Cleanup() override { return CraneErrCode::SUCCESS; }

  std::vector<int> signals;
  CraneErrCode result{CraneErrCode::SUCCESS};
};

class NonRecursiveCgroup : public Common::CgroupInterface {
 public:
  NonRecursiveCgroup()
      : Common::CgroupInterface("non-recursive-test-cgroup", nullptr) {}
  bool KillAllProcesses(int) override {
    ++parent_kill_calls;
    return true;
  }
  bool Empty() override { return true; }
  bool SetCpuCoreLimit(double) override { return false; }
  bool SetCpuShares(uint64_t) override { return false; }
  bool SetCpuSet(const std::unordered_set<uint32_t>&) override { return false; }
  bool SetCpusetMems(const std::string&) override { return false; }
  bool SetMemoryLimitBytes(uint64_t) override { return false; }
  bool SetMemorySwLimitBytes(uint64_t) override { return false; }
  bool SetMemorySoftLimitBytes(uint64_t) override { return false; }
  bool SetBlockioWeight(uint64_t) override { return false; }
  bool SetDeviceAccess(const std::unordered_set<SlotId>&, bool, bool,
                       bool) override {
    return false;
  }
  int parent_kill_calls{0};
  int subtree_kill_calls{0};
  bool KillSubtree() override {
    ++subtree_kill_calls;
    return false;
  }
};

TEST(CrunResidualCleanup, WaitsForLastExitAndRetriesFailedTaskCleanup) {
  StepToSupv spec;
  spec.set_type(crane::grpc::Interactive);
  spec.mutable_interactive_meta()->set_interactive_type(crane::grpc::Crun);
  StepInstance step(spec);
  auto first = std::make_unique<ResidualTask>(&step, 0);
  auto last = std::make_unique<ResidualTask>(&step, 1);
  auto& a = *first;
  auto& b = *last;
  step.AddTaskInstance(0, std::move(first));
  step.AddTaskInstance(1, std::move(last));
  step.step_user_cg = std::make_unique<Common::CgroupV1>("test", nullptr);

  a.GetFinalInfo()->raw_exit.emplace();
  step.CleanResidualProcesses();
  EXPECT_TRUE(a.signals.empty());
  EXPECT_TRUE(b.signals.empty());

  b.GetFinalInfo()->raw_exit.emplace();
  b.result = CraneErrCode::ERR_GENERIC_FAILURE;
  step.CleanResidualProcesses();
  EXPECT_EQ(a.signals, std::vector<int>{SIGKILL});
  EXPECT_EQ(b.signals, std::vector<int>{SIGKILL});

  b.result = CraneErrCode::SUCCESS;
  step.CleanResidualProcesses();
  step.CleanResidualProcesses();
  EXPECT_EQ(a.signals, (std::vector<int>{SIGKILL, SIGKILL}));
  EXPECT_EQ(b.signals, (std::vector<int>{SIGKILL, SIGKILL}));
}

TEST(CrunResidualCleanup, FallsBackWhenCgroupV2BackendIsNotRecursive) {
  StepToSupv spec;
  spec.set_type(crane::grpc::Interactive);
  spec.mutable_interactive_meta()->set_interactive_type(crane::grpc::Crun);
  StepInstance step(spec);
  auto task = std::make_unique<ResidualTask>(&step, 0);
  auto& process = *task;
  process.GetFinalInfo()->raw_exit.emplace();
  step.AddTaskInstance(0, std::move(task));
  auto cgroup = std::make_unique<NonRecursiveCgroup>();
  auto& parent = *cgroup;
  step.step_user_cg = std::move(cgroup);

  step.CleanResidualProcesses();

  EXPECT_EQ(parent.subtree_kill_calls, 1);
  EXPECT_EQ(parent.parent_kill_calls, 0);
  EXPECT_EQ(process.signals, std::vector<int>{SIGKILL});
}

}  // namespace

namespace {

class OomTerminationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "crane-oom-result-XXXXXX")
            .string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    root_ = pattern;
    job_ = root_ / "job";
    cg_ = job_ / "step_1/user";
    std::filesystem::create_directories(cg_);
    spec_.set_type(crane::grpc::Batch);
    spec_.set_step_type(crane::grpc::PRIMARY);
  }

  void TearDown() override { std::filesystem::remove_all(root_); }

  static void Write(const std::filesystem::path& path, std::string_view text) {
    std::ofstream file(path, std::ios::trunc);
    ASSERT_TRUE(file.is_open());
    file << text;
    ASSERT_TRUE(file.good());
  }

  void InitCounter(StepInstance& step) {
    Write(cg_ / "memory.events", "oom 0\noom_kill 0\n");
    ASSERT_TRUE(step.oom_monitor.Start(cg_, job_, true));
  }

  void Oom() { Write(cg_ / "memory.events", "oom 1\noom_kill 1\n"); }

  static TaskFinalInfo Exit(int value = 0, bool signaled = false) {
    return {.raw_exit = TaskExitInfo{.is_terminated_by_signal = signaled,
                                     .value = value}};
  }

  std::filesystem::path root_, job_, cg_;
  StepToSupv spec_;
};

TEST_F(OomTerminationTest, OomWinsOverSuccessfulShellExitAndPreservesReason) {
  StepInstance step(spec_);
  InitCounter(step);
  Oom();
  // Exactly the result of a shell which prints its OOM-killed child's status
  // and then exits successfully. No synthetic signal/exit code is introduced.
  step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
  const auto& result = step.final_termination_status;
  EXPECT_EQ(result.final_status_on_termination, StepStatus::OutOfMemory);
  EXPECT_EQ(result.max_exit_code, 0);
  EXPECT_NE(result.final_reason_on_termination.find("oom_kill"),
            std::string::npos);
}

TEST_F(OomTerminationTest, OomPreservesNonzeroAndSignalExitCodes) {
  for (bool signal : {false, true}) {
    StepInstance step(spec_);
    InitCounter(step);
    Oom();
    auto info = Exit(signal ? SIGKILL : 137, signal);
    uint32_t code = signal ? ExitCode::kTerminationSignalBase + SIGKILL : 137;
    step.RecordTaskResult(StepStatus::Failed, code, std::nullopt, info);
    EXPECT_EQ(step.final_termination_status.final_status_on_termination,
              StepStatus::OutOfMemory);
    EXPECT_EQ(step.final_termination_status.max_exit_code, code);
  }
}

TEST_F(OomTerminationTest, OrdinaryExitAndSigkillAreNotEvidenceOfOom) {
  for (auto code :
       {0U, 137U, uint32_t(ExitCode::kTerminationSignalBase + SIGKILL)}) {
    StepInstance step(spec_);
    InitCounter(step);
    auto status = code == 0 ? StepStatus::Completed : StepStatus::Failed;
    step.RecordTaskResult(status, code, std::nullopt, Exit());
    EXPECT_EQ(step.final_termination_status.final_status_on_termination,
              status);
    EXPECT_EQ(step.final_termination_status.max_exit_code, code);
  }
}

TEST_F(OomTerminationTest, MonitoringFailureDoesNotInventOom) {
  StepInstance step(spec_);
  InitCounter(step);
  std::filesystem::remove(cg_ / "memory.events");
  step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
  EXPECT_EQ(step.final_termination_status.final_status_on_termination,
            StepStatus::Completed);
}

TEST_F(OomTerminationTest, OomSurvivesOtherTaskFailureInEitherOrder) {
  for (bool oom_first : {false, true}) {
    StepInstance step(spec_);
    InitCounter(step);
    auto failure = [&] {
      TaskFinalInfo info{.cause = TaskFinalizeCause::TASK_SPAWN_FAILED};
      step.RecordTaskResult(StepStatus::Failed, ExitCode::EC_SPAWN_FAILED,
                            "spawn failure", info);
    };
    if (!oom_first) failure();
    Oom();
    step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
    if (oom_first) failure();
    EXPECT_EQ(step.final_termination_status.final_status_on_termination,
              StepStatus::OutOfMemory);
    EXPECT_NE(step.final_termination_status.final_reason_on_termination.find(
                  "oom_kill"),
              std::string::npos);
  }
}

TEST_F(OomTerminationTest, ExplicitTerminationKeepsPrecedenceOverOom) {
  for (const auto& [cause, status] :
       {std::pair{TaskFinalizeCause::CANCELLED_BY_USER, StepStatus::Cancelled},
        std::pair{TaskFinalizeCause::TIMEOUT, StepStatus::ExceedTimeLimit},
        std::pair{TaskFinalizeCause::DEADLINE, StepStatus::Deadline},
        std::pair{TaskFinalizeCause::CFORED_DISCONNECTED,
                  StepStatus::Failed}}) {
    for (bool oom_first : {false, true}) {
      StepInstance step(spec_);
      InitCounter(step);
      Oom();
      auto normal = [&] {
        step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
      };
      if (oom_first) normal();
      auto info = Exit(9, true);
      info.cause = cause;
      step.RecordTaskResult(status, 265, "explicit termination", info);
      if (!oom_first) normal();
      EXPECT_EQ(step.final_termination_status.final_status_on_termination,
                status);
      EXPECT_EQ(step.final_termination_status.final_reason_on_termination,
                "explicit termination");
    }
  }
}

TEST_F(OomTerminationTest, CallocKeepsExistingCompletionSemantics) {
  spec_.set_type(crane::grpc::Interactive);
  spec_.mutable_interactive_meta()->set_interactive_type(crane::grpc::Calloc);
  StepInstance step(spec_);
  InitCounter(step);
  Oom();
  step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
  EXPECT_EQ(step.final_termination_status.final_status_on_termination,
            StepStatus::Completed);
}

TEST_F(OomTerminationTest, OldKernelNotificationHandlesShellExitZero) {
  for (bool ancestor : {false, true}) {
    for (bool primary : {false, true}) {
      spec_.set_step_type(primary ? crane::grpc::PRIMARY : crane::grpc::COMMON);
      spec_.set_type(primary ? crane::grpc::Batch : crane::grpc::Interactive);
      spec_.mutable_interactive_meta()->set_interactive_type(crane::grpc::Crun);
      StepInstance step(spec_);
      for (const auto& path : {job_, cg_}) {
        Write(path / "memory.oom_control", "oom_kill_disable 0\nunder_oom 0\n");
        Write(path / "cgroup.event_control", "");
      }
      ASSERT_TRUE(step.oom_monitor.Start(cg_, job_, false));
      auto notify = [&](const auto& path) {
        std::ifstream input(path / "cgroup.event_control");
        int fd = -1;
        input >> fd;
        ASSERT_GE(fd, 0);
        ASSERT_EQ(eventfd_write(fd, 1), 0);
      };
      notify(cg_);
      if (ancestor) notify(job_);
      step.oom_monitor.Stop();
      step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt, Exit());
      EXPECT_EQ(step.final_termination_status.final_status_on_termination,
                !ancestor || primary ? StepStatus::OutOfMemory
                                     : StepStatus::Completed);
      EXPECT_EQ(step.final_termination_status.max_exit_code, 0);
      EXPECT_FALSE(
          step.final_termination_status.final_reason_on_termination.empty());
      if (ancestor)
        EXPECT_NE(
            step.final_termination_status.final_reason_on_termination.find(
                "victim step is unknown"),
            std::string::npos);
    }
  }
}

}  // namespace

}  // namespace Craned::Supervisor
