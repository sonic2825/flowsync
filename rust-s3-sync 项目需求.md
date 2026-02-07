
---

### 1. 核心技术栈与架构 (Architecture)

- **开发语言:** Rust (利用其内存安全、无GC停顿、高性能并发特性)。
    
- **核心库:** `apache/opendal` (作为统一的存储访问层)。

- **异步运行时:** `tokio` (处理高并发 IO 操作)。
    
- **CLI 框架:** `clap` (用于构建强大的命令行参数解析)。
    

### 2. 存储后端支持 (Storage Backends)

基于 `opendal` 的能力，你需要明确各协议的具体对接方式：

- **本地文件系统 (Fs):** 支持标准 POSIX 文件操作。
    
- **对象存储 (S3):** 需兼容 AWS S3 及其他兼容 S3 的服务（如 MinIO, Aliyun OSS, COS, Cloudflare R2）。需支持 Region、Endpoint、AccessKey/SecretKey 配置。
    
- **SFTP:** 支持基于 SSH 的文件传输，需支持密码认证和密钥认证。
    
- **FTP:** 支持标准 FTP 协议（主动/被动模式）。
    
- **NFS 特别说明:** `opendal` 目前主要通过 `fs` service 支持挂载在本地的 NFS 路径。如果需要原生 NFS 协议客户端（不依赖系统挂载），可能需要额外的 Rust crate，建议初期版本**通过本地挂载路径（fs）支持 NFS**。
    

### 3. 核心传输与同步逻辑 (Core Logic)

这部分是工具的灵魂，需要细分为以下功能点：

#### 3.1 同步模式 (Sync Modes)

- **Copy (复制):** 源到目标，如果目标存在且相同则跳过。
    
- **Sync (镜像同步):** 使目标与源完全一致。**注意：这包含删除操作**（如果源没有，目标有，则删除目标文件）。
    
- **Move (移动):** 复制后删除源文件。
    

#### 3.2 差异检测 (Change Detection)

- **ModTime + Size (默认):** 对比文件大小和修改时间。这是最高效的方式。
    
- **Checksum (哈希校验):** 可选开启。计算 MD5/SHA256 进行对比（更可靠但消耗 CPU 和 IO）。
    
- **Ignore Existing:** 只要目标存在，无论是否修改都不覆盖。
    

#### 3.3 大小文件专项优化 (Optimization)

- **大量小文件 (High IOPS):**
    
    - 实现**并发扫描 (Concurrent Listing)**：多线程列出目录结构。
        
    - 实现**并发传输 (Concurrent Transfer)**：同时上传 N 个小文件（利用 `tokio` 的 semaphore 控制并发数）。
        
- **超大文件 (High Throughput):**
    
    - **分片上传 (Multipart Upload):** 利用 S3 等协议的分片特性，断点续传的基础。
        
    - **流式传输 (Streaming):** 避免一次性将大文件加载到内存，需实现 `Reader` 到 `Writer` 的流式拷贝。
        

### 4. 命令行交互设计 (CLI Design)

一个优秀的 CLI 工具必须具备良好的交互体验。

#### 4.1 配置文件管理

- 支持 `rclone config` 类似的交互式配置生成（Wizard 模式）。
    
- 支持从环境变量读取密钥（方便 CI/CD 集成）。
    
- 配置文件格式建议使用 TOML。
    

#### 4.2 常用命令结构

Bash

```
mytool copy local:/path/src s3:bucket/dest
mytool sync s3:bucket/src sftp:host/dest
mytool ls s3:bucket/  # 列出文件
```

#### 4.3 关键参数 (Flags)

- `--dry-run`: **(至关重要)** 只打印会发生什么（复制、删除），不实际执行。
    
- `--transfers N`: 同时并行的文件传输数量（默认例如 4）。
    
- `--checkers N`: 同时并行的检查/扫描数量。
    
- `--bandwidth-limit`: 限制传输速度（如 10M），防止占满带宽。
    
- `--include/--exclude`: 支持 Glob 模式匹配（如 `*.log`, `!important.txt`）。
    
### 5. 完善的配置管理系统：

- **配置文件支持：** 定义基于 TOML 的配置文件格式，支持保存多个 "Remote"（远程端点）。
    
- **S3 深度适配：** 必须支持自定义 Endpoint、Region 和 URL Style，以兼容 AWS、MinIO、阿里云 OSS 等主流对象存储。
    
- **交互式向导：** 开发 `config` 子命令，通过问答方式引导用户生成配置文件（使用 `dialoguer` 库），降低手写配置的错误率。
    
- **安全：** (可选进阶) 支持对配置文件进行加密，保护 AccessKey。

### 6. 可靠性与容错 (Reliability)

- **重试机制 (Retries):** 遇到网络抖动或临时错误（HTTP 5xx, Socket Timeout）时，必须实现指数退避（Exponential Backoff）重试。
    
- **原子性保障:** 尽量确保文件传输完整后再重命名为最终文件名（如果后端支持），防止产生损坏的半截文件。
    
- **断点续传:** 针对大文件，如果传输中断，下次启动能从断点继续（主要针对 S3 Multipart 和 HTTP Range）。
    

### 7. 可观测性 (Observability)

- **进度条:** 使用 `indicatif` 库，展示：
    
    - 总进度（文件数/总大小）。
        
    - 当前传输速度。
        
    - 预计剩余时间 (ETA)。
        
- **日志记录:**
    
    - 支持 `--log-level` (debug, info, error)。
        
    - 传输失败的文件必须单独记录，方便后续排查。
        

---

### 建议的开发路线图 (Roadmap)

1. **Phase 1 (MVP):** 仅支持 Local 和 S3，实现 `copy` 命令，支持简单的 Size 对比，无并发。
    
2. **Phase 2 (Concurrency):** 引入 `tokio` 并发模型，实现 `--transfers` 参数，支持大量小文件加速。
    
3. **Phase 3 (Protocols):** 接入 FTP/SFTP，完善配置管理系统。
    
4. **Phase 4 (Advanced):** 实现 `sync`（带删除逻辑）、`--dry-run`、进度条、断点续传。
    
