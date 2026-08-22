# 鹤思 Skill

鹤思 Skill 是一组供 AI 助手读取的 CraneSched 使用与一线排障指南。它可以帮助用户：

- 编写和检查 `cbatch`、`calloc`、`crun` 等作业命令与脚本；
- 理解队列、作业状态、排队原因和退出码；
- 排查资源请求、GPU、MPI、多节点、容器和作业失败问题；
- 区分用户可自行检查的内容与需要交给管理员的集群问题。

Skill 不会授予额外权限，也不会代替集群的用户手册和策略。回答应以当前安装版本的
`--help`、本站文档和用户实际输出为准。

## 谁可以使用

所有鹤思用户都可以把 Skill 安装到自己的 AI 助手中。它与
[集群共享 Codex](shared-codex.md) 相互独立：即使管理员没有部署集群版 Codex，
用户仍可在自己的 Codex、Claude Code 或其他能读取 `SKILL.md` 的 Agent 中使用。

## 安装

Skill 的权威源文件位于
[`docs/skills/cranesched-skill`](https://github.com/PKUHPC/CraneSched/tree/master/docs/skills/cranesched-skill)。
安装时必须保留整个目录，不能只复制 `SKILL.md`，因为其中的 `references/` 也是
回答所需资料。

下面的示例将仓库稀疏克隆到用户目录，并安装到通用的 Agent Skills 目录：

```bash
skill_source="${XDG_DATA_HOME:-$HOME/.local/share}/cranesched-skill-source"
git clone --depth 1 --filter=blob:none --sparse \
  https://github.com/PKUHPC/CraneSched.git "${skill_source}"
git -C "${skill_source}" sparse-checkout set docs/skills/cranesched-skill
mkdir -p "${HOME}/.agents/skills"
ln -s "${skill_source}/docs/skills/cranesched-skill" \
  "${HOME}/.agents/skills/cranesched-skill"
```

- **Codex 或支持 Agent Skills 的工具**：使用 `~/.agents/skills/`，或按工具文档安装到其用户级 Skill 目录。
- **Claude Code**：可将最后两条命令中的 `~/.agents/skills` 改为 `~/.claude/skills`。
- **不自动发现 Skill 的 Agent**：保留源目录，并明确要求它读取其中的 `SKILL.md` 后再处理鹤思问题。

重新启动 Agent 或开启新会话后，可以直接询问“帮我写一个鹤思 GPU 作业脚本”，
或显式要求使用 `cranesched-skill`。更新 Skill 时运行：

```bash
git -C "${skill_source}" pull --ff-only
```
