# 本地 WTelegramClient 补丁

- 上游仓库：https://github.com/wiz0u/WTelegramClient
- 基准提交：`187c773a8f875db9f4f76e0778a33d025c0b70a1`
- 本项目使用版本：`4.4.9`，协议 layer 229。
- 保留上游 MIT 许可证、协议代码、加密安全检查及 `generator/MTProtoGenerator.csproj` 源生成器。

## reCAPTCHA 错误原文保留

上游 `Client.Invoke` 会将 RPC 错误中下划线后的数字替换为 `X`，以识别
`FLOOD_WAIT_60`、`PHONE_MIGRATE_4` 等携带数字参数的错误。但 reCAPTCHA 的错误格式是
`RECAPTCHA_CHECK_<action>__<site_key>`，其中数字是 action/site key 的一部分。
例如 `__6Lfs...` 会被错误改为 `__XLfs...`；较长数字片段还可能导致整数解析溢出。

本地修改将原有逻辑抽为私有纯函数 `NormalizeRpcError`，只对
`RECAPTCHA_CHECK_` 前缀保留完整原文，并令数字参数 `X` 为 `-1`。
其余 RPC 错误沿用原有数字归一化逻辑；没有改变迁移、限流重试或协议校验。
回归测试位于 `TelegramLogin.Tests/WTelegramCaptchaErrorTests.cs`，反射仅用于测试私有函数，
生产路径没有增加反射。

## 项目构建调整

SDK 项目只构建 `net8.0`，由主应用的 `net10.0` 引用；版本设为 `4.4.9`。
协议源生成器仍构建 `netstandard2.0`，继续作为 analyzer 引用。
源生成器的项目引用移除主应用传入的 AOT、单文件、自包含和目标平台发布属性，
避免 Windows AOT 发布将只在编译期运行的 `netstandard2.0` analyzer 也按原生应用发布（NETSDK1207）。
NuGet 的还原图还会直接传入命令行全局属性，因此 generator 工程也用 `TreatAsLocalProperty`
明确保留普通托管 analyzer 的构建模式；主应用的 AOT 发布保持开启。
没有新增警告屏蔽。上游 `TL.SchemaFuncs.cs` 中 7 条未能解析的 XML `cref` 注释会产生
`CS1574` 警告，与本次逻辑补丁无关。

## Debug 构建保持生产 DC

上游源码在 `DEBUG` 条件下默认连接测试 DC `2>149.154.167.40:443`，而原来使用的
NuGet 发布程序集默认连接生产 DC。改为本地项目引用后，主应用的 Debug 构建会同步编译
SDK，从而意外切换到测试环境。

本地修改让 `DefaultConfig("server_address")` 在所有构建配置中统一返回生产 DC
`2>149.154.167.50:443`。需要测试 DC 时，调用方仍可通过既有配置回调显式提供地址。
没有改变地址解析、数据中心迁移或会话恢复逻辑。

其他 Debug 分支保持原样：KeepAlive 的 `PingDelayDisconnect` 断线容忍时长是
`PingInterval * 5`（Release 为 `PingInterval * 5 / 4`）；TL 的不应到达分支在 Debug
触发 `Debugger.Break`，在 Release 抛异常。没有关闭断言或安全检查。

## 盐刷新跟随连接生命周期

上游 `CheckSalt` 使用不带取消的五秒延迟，然后在嵌套 continuation 中读取
`gfs.Result.salts`。代理测试等短连接已释放时，延迟任务仍调用 `GetFutureSalts`，
引发 `You must connect to Telegram first`，随后 `.Result` 将它包装成未处理的
`AggregateException`。

本地修改使用可跟踪的异步任务，捕获连接的取消令牌、DC 和独立操作标识。
Reset/Dispose 取消任务并清理刷新标记；旧任务不能写入新连接或删除新任务的标记。
加载会话时若保留了没有活跃任务的旧标记，`CheckSalt` 会移除它，避免永久跳过刷新。
Reset 不等待刷新任务，避免接收循环、迁移及释放过程互相等待。

刷新使用单次 `get_future_salts` 服务请求，正常响应仍更新并保存盐值；失败记录日志，
允许后续 `CheckSalt` 再次刷新。该维护请求不进入普通 `Invoke` 的迁移和自动重试流程。
取消令牌透传到发送、信号量等待和 HTTP 路径，普通 RPC 和公开 API 签名保持不变。
取消后只清理自己仍持有的 pending RPC；已被接收器取走的请求继续观察其结果或异常，
避免与接收器争抢完成任务或遗留未观察异常。

测试位于 `TelegramLogin.Tests/WTelegramSaltLifetimeTests.cs`，使用内存会话和模拟传输，
不登录账号、不发送验证码。旧实现的“释放后不应再发请求”测试失败（实际请求数为 1），
修复后正常刷新、重复触发、发送失败、取消、同 DC 重连、迟到响应及 RPC 错误等用例通过。

## 暴露会话实际使用的 API ID

新增只读属性 `Client.ApiId`，返回 `_session.ApiId`，供应用在联网前检查配置与会话是否匹配。
上游只有在会话尚未保存 API ID 时才读取配置；因此修改界面 API ID 后复用原会话，
原先可能继续使用旧 ID。应用现在遇到不匹配会关闭刚创建的客户端并提示选择匹配配置或
新建会话，保留原文件，不自动改写身份或注销账号。生产路径不依赖反射。

回归使用合成的加密会话，验证匹配/不匹配、保留原文件及释放文件锁。
这修正了配置与诊断的一致性，不能作为 Telegram 本次人机挑战的触发原因证明。

## 验证记录

针对原始算法运行回归测试时，7 个 reCAPTCHA 用例失败，8 个普通 RPC 用例通过。
应用前缀例外后，15 个用例全部通过，包括数字开头的 key、key 内数字片段、前导零、
超长数字，以及 FLOOD_WAIT、SLOWMODE_WAIT、PHONE/NETWORK/FILE_MIGRATE 等既有行为。
测试不连接 Telegram、不请求真实验证码。

增加默认 DC 回归测试后，修复前 Debug 构建结果为 1 失败、15 通过；统一生产默认地址后，
Debug 和 Release 两种配置的 16 个用例均全部通过。

盐生命周期修复后，全量 C# 测试 195 项通过，其中 14 项覆盖本次刷新与取消逻辑；
本次修改的可执行代码行覆盖率为 89.0%（81/91）。真实直连初次握手失败，使用旧版
NativeAOT 程序与新版托管程序对照重测后均连接成功；新版约 3.6 秒完成连接，释放后
继续观察 7 秒并触发 GC，未捕获断开后发送或未观察任务异常。没有登录账号或发送验证码。
