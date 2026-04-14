# 0G OpenClaw

[English](README.md) | 简体中文

<p align="center">
        <img src="img/0g-logo.svg" alt="0G OpenClaw logo" width="320">
</p>

**Your key. Your agent.**

0G OpenClaw 不是“又一个需要 API key 的 AI 工具”，而是一个跟着你的钱包走的个人 Agent。

- 只要一个钱包和一点 $0G，你就可以在任意一台机器上唤回同一个 Agent：同样的记忆、同样的上下文、同样的身份。
- 不需要购买大模型API，可以直接使用0G Compute Market的模型，其输出安全性由0G TEE保障。
- 基于 OpenClaw 研发，你可以享受到OpenClaw完整的强大功能

这个仓库把这个产品想法和背后的本地 KV 工作流打包在同一个 workspace 里：

1. `0g_openclaw`：一个集成了 0G Storage、0G KV 内存同步与恢复能力 + 0G Compute Network 的定制版 OpenClaw 代码库。
2. `0g-storage-kv-local`：0G Storage KV 节点的本地源码副本，用于参考、调试，以及在需要时作为源码构建回退方案。

这个项目最核心的目标很直接：不要让 Agent 因为你换了一台电脑、重装了一次环境，或者不想买中心化 API，就变成另一个全新的助手。钱包是锚点，0G 是记忆通道，OpenClaw 则变成一个可以在任何地方重新唤回的产品。

<p align="center">
        <img src="img/screen.png" alt="0G OpenClaw logo">
</p>

Demo Video: https://www.youtube.com/watch?v=UQuXa4rrlTs

## 仓库结构

```text
0ghack/
├── README.md
├── README.zh-CN.md
├── .gitignore
├── 0g_openclaw/
└── 0g-storage-kv-local/
```

### `0g_openclaw/`

这是主应用仓库。它是一个以 OpenClaw CLI、gateway、UI、extensions，以及移动端和桌面端客户端为核心的 Node.js / TypeScript monorepo。其完全继承 OpenClaw 的强大功能，同时集成了与 0G 相关的重要增强：

- 在产品流程中加入 0G 钱包与账户感知。
- 0G memory upload 与 restore 服务。
- 模型服务更换为 0G Compute Network。
- 位于 `scripts/dev/` 下的本地 KV 辅助脚本。

### `0g-storage-kv-local/`

这是 0G Storage KV 节点源码的本地副本。之所以把它放进工作区，是为了让整个 workspace 自带理解、检查和必要时从源码构建 KV 节点所需的全部内容。

需要注意：

- 它是一个 Rust workspace，不是 pnpm package。
- 它不属于 OpenClaw 的 pnpm workspace。
- OpenClaw 辅助脚本在 macOS 上会优先下载官方预编译 `zgs_kv` binary，只有必要时才会回退到源码构建。

## 0G 记忆架构

这个仓库里的 memory sync 设计大致如下：

1. OpenClaw 扫描本地 memory 文件与 session transcript。
2. 把变更过的文件上传到 0G Storage。
3. 将描述这些文件的 manifest 上传到 0G Storage。
4. 把 `latest-manifest` 指针写入 0G KV。
5. 恢复时先读取这个 KV 指针，再下载 manifest，然后下载并重写缺失的本地文件。

概念上可以理解为：

```text
本地 transcripts / memory
        |
        v
0G Storage blobs
        |
        v
0G Storage 中的 Manifest
        |
        v
0G KV 中的 latest-manifest 指针
        |
        v
恢复回 ~/.openclaw
```

这套实现的关键行为包括：

- Memory sync 是 local-first，而不是 cloud-first。
- Restore 依赖基于钱包地址派生出的确定性 stream ID。
- Startup restore 的设计目标，是在本地记忆索引初始化之前完成恢复，这样恢复出的文件能立即被看到。
- 如果缺少 KV read 支持，restore 应当安全跳过，而不是让整个 setup 失败。
## 安装与开发

### 前置要求

对于 OpenClaw 这一侧：

- 推荐 Node.js 24
- pnpm

对于可选的 KV 源码构建路径：

- Rust toolchain
- cargo
- 0G KV 项目所需的平台原生构建依赖

### 安装 OpenClaw 依赖

根仓库本身不是一个 pnpm workspace package。应用侧依赖应当在 `0g_openclaw` 目录内安装：

```bash
cd 0g_openclaw
pnpm install
```

这是当前项目里安装应用侧依赖的标准方式。

### 常见本地使用流程

```bash
cd 0g_openclaw
pnpm install

# 初始配置
pnpm openclaw onboard

# 启动或验证本地 KV 节点
scripts/dev/start-local-zerog-kv.sh

# 将本地记忆推送到 0G
pnpm openclaw zerog sync

# 将远端记忆拉回本地状态
OPENCLAW_0G_KV_RPC_URL=http://127.0.0.1:6789 pnpm openclaw zerog restore
```

## 面向用户的主要命令

所有 OpenClaw 命令都应该在 `0g_openclaw` 目录内运行。

### 将本地记忆上传到 0G

```bash
cd 0g_openclaw
pnpm openclaw zerog sync
```

这个命令会上传发生变化的 transcript 和 memory 文件，并更新远端 manifest 指针。

### 手动从 0G 恢复记忆

```bash
cd 0g_openclaw
OPENCLAW_0G_KV_RPC_URL=http://127.0.0.1:6789 pnpm openclaw zerog restore
```

如果你想强制覆盖本地已有同名文件：

```bash
cd 0g_openclaw
OPENCLAW_0G_KV_RPC_URL=http://127.0.0.1:6789 pnpm openclaw zerog restore --force
```

这个命令是专门为手动拉取远端状态而加的，这样用户就不必只依赖 onboarding 或 startup 的时机，才能把远端内容恢复到一个干净的本地安装中。

## 本地 KV 工作流

仓库中包含了一组辅助脚本，用来让 OpenClaw 侧的本地 KV 工作流真正可用：

- `0g_openclaw/scripts/dev/start-local-zerog-kv.sh`
- `0g_openclaw/scripts/dev/stop-local-zerog-kv.sh`
- `0g_openclaw/scripts/dev/openclaw-with-local-zerog-kv.sh`

### 启动本地 KV 节点

```bash
cd 0g_openclaw
scripts/dev/start-local-zerog-kv.sh
```

它会做这些事情：

- 从 `~/.openclaw/credentials/ethereum-wallet.json` 读取 OpenClaw 钱包。
- 推导钱包地址。
- 推导 OpenClaw memory stream ID。
- 生成本地 KV 配置。
- 优先下载官方 `zgs_kv` macOS 预编译 binary。
- 只有在必要时才回退到源码构建。
- 启动本地 KV RPC 服务。

### 让某个 OpenClaw 命令自动接上本地 KV RPC 再运行

```bash
cd 0g_openclaw
scripts/dev/openclaw-with-local-zerog-kv.sh pnpm openclaw zerog restore
```

这个包装脚本会先确保本地 KV 服务已启动，然后导出 `OPENCLAW_0G_KV_RPC_URL`，再执行你传入的命令。

## 当前推荐的运行方式

目前推荐的 restore 模式如下：

1. 在本地运行 OpenClaw。
2. 让它指向一个真正支持 `kv_getValue` 的 KV 端点。
3. 能用 onboarding 或 startup restore 时就优先使用。
4. 当你需要确定性的手动拉取时，使用 `openclaw zerog restore`。

这在以下场景中尤其有用：

- 重装 OpenClaw 之后，
- 切换机器之后，
- 使用一个新的本地 state directory 之后，
- 本地自建 KV 节点完成相关链历史回放、刚刚可以提供正确查询之后。

## 关于随仓库包含的 0G KV 源码树

之所以把 `0g-storage-kv-local` 放进来，是为了让整个本地 KV 工作流在一个仓库中可见、可查、可复现。它适合用于：

- 检查源码，
- 调试，
- 验证配置格式，
- 在预编译二进制不可用时进行源码构建，
- 让 hackathon 提交物更完整。

它应当被视为一个被包含进来的上游源码树，而不是 OpenClaw pnpm workspace 内部的一个 package。

## 来源说明

- `0g_openclaw` 基于 OpenClaw，再在这个 hackathon workspace 中进行扩展。
- `0g-storage-kv-local` 是 0G Storage KV 仓库的本地源码副本。
- 
## 许可证与上游归属

这个仓库中包含多个上游代码库。每个子项目都保留了自己的 license 与 attribution 文件。在重新分发或复用代码之前，请分别查看各子目录中的许可证文件。