# FlashGo Plus（黑马点评 Plus）项目速查手册

## 一、项目概述

| 项目 | 说明 |
|------|------|
| **名称** | flashgo-plus（黑马点评 Plus 升级版） |
| **类型** | 企业级高并发电商点评平台 |
| **核心场景** | 优惠券秒杀、热点查询、高并发稳定性 |
| **技术栈** | Spring Boot 3.5.4 + Vue 3 + Kafka + Redis + MySQL + ShardingSphere |
| **Java 版本** | JDK 17+ |
| **一句话定位** | 高并发秒杀电商点评平台。核心解决：库存超卖、缓存与DB不一致、MQ消息丢失、突发流量击垮系统。技术路线：Redis+Lua原子扣减 → Kafka异步下单 → Outbox+对账保证最终一致性 |

黑马点评是"本地生活与商户运营"场景的综合实战项目，包含商户浏览与查询、优惠券发放与抢购、达人探店、好友关注、签到与UV统计等核心业务。Plus 版本将 SpringBoot 升级到 3.5.4，所有中间件升级到最新版本，补齐高并发稳定性、限流、数据一致性保障、可观测与故障闭环等企业级能力。

---

## 二、模块结构（15 个子模块，插件化架构）

```
flashgo-plus
├── flashgo-common                          # 公共枚举/异常/工具类
├── flashgo-core-service                    # 主业务入口 (Spring Boot, :8085)
├── flashgo-sharding                        # ShardingSphere-JDBC 分库分表
├── flashgo-parameter                       # DTO/VO 参数定义
├── flashgo-id-generator-framework          # 雪花算法 + Redis 注册 workerId
├── flashgo-redis-tool-framework/
│   ├── flashgo-redis-common-framework      #   Redis 自动配置
│   ├── flashgo-redis-framework             #   缓存封装 (Cache-Aside)
│   └── flashgo-redis-rate-limit-framework  #   令牌桶/滑动窗口限流
├── flashgo-redisson-framework/
│   ├── flashgo-bloom-filter-framework      #   声明式布隆过滤器
│   ├── flashgo-redisson-common-framework   #   Redisson 公共配置
│   ├── flashgo-repeat-execute-limit-framework  # @RepeatExecuteLimit 幂等
│   ├── flashgo-service-lock-framework      #   @ServiceLock 分布式锁
│   └── flashgo-service-delay-queue-framework   # 延迟队列
├── flashgo-mq-framework/
│   ├── flashgo-mq-common-framework         #   MessageExtend 消息信封
│   ├── flashgo-mq-producer-framework       #   AbstractProducerHandler 模板
│   └── flashgo-mq-consumer-framework       #   AbstractConsumerHandler 模板
└── flashgo-vue3                            # 前端 (Vue 3 + Element Plus, :5173)
```

**依赖原则**：`flashgo-core-service` 依赖所有框架模块，框架模块之间互不依赖——任一根架模块可被独立替换。

---

## 三、技术栈

### 后端

| 技术 | 版本 | 用途 |
|------|------|------|
| Spring Boot | 3.5.4 | 应用框架 |
| MyBatis Plus | 3.5.7 | ORM |
| ShardingSphere JDBC | 5.3.2 | 分库分表 (2库：flashgo_0, flashgo_1) |
| Redisson | 3.52.0 | 分布式锁、布隆过滤器、延迟队列 |
| Kafka | — | 异步消息（秒杀订单处理） |
| Redis | 6.0.8 | 缓存、限流、会话、BitMap签到、HyperLogLog UV统计、GEO附近商户 |
| Caffeine | — | 本地进程内缓存 |
| Sa-Token | 1.43.0 | 权限认证 |
| Knife4j | 4.3.0 | API 文档 |
| Hutool | 5.8.25 | 工具库 |
| Micrometer + Prometheus | — | 可观测性指标 |
| FastJSON | 2.0.9 | JSON 处理 |
| Log4j2 | 2.17.0 | 日志 |

### 前端

| 技术 | 版本 | 用途 |
|------|------|------|
| Vue | 3.5.13 | 前端框架 |
| Vue Router | 4.5 | 路由 |
| Pinia | 3.0 | 状态管理 |
| Element Plus | 2.9 | UI 组件库 |
| Axios | 1.8 | HTTP 客户端 |
| Vite | 6.2 | 构建工具 |

---

## 四、数据库架构（分库分表）

### 分片表（8 张）

| 表名 | 分库键 | 分表键 | 说明 |
|------|--------|--------|------|
| tb_voucher_order | user_id | voucher_id | 订单（user_id 分库保证我的订单单库路由，voucher_id 分表打散秒杀热点） |
| tb_voucher_order_router | order_id | order_id | 订单路由表（解决按 order_id 查订单的非分片键查询） |
| tb_seckill_voucher | voucher_id | voucher_id | 秒杀优惠券 |
| tb_voucher | id | id | 普通优惠券 |
| tb_user | id | id | 用户 |
| tb_user_info | user_id | user_id | 用户信息 |
| tb_user_phone | phone (HASH_MOD) | phone (HASH_MOD) | 用户手机号（支持手机号反向查询） |
| tb_voucher_reconcile_log | order_id | order_id | 对账日志 |

### 广播表（7 张，每个库全量副本）

`tb_blog`, `tb_blog_comments`, `tb_follow`, `tb_rollback_failure_log`, `tb_shop`, `tb_shop_type`, `tb_sign`

### 跨库查询策略

- **路由表**：tb_voucher_order_router，两步查询替代跨库 JOIN
- **广播表**：基础数据每库全量，支持库内 JOIN
- **应用层组装**：Java 层分别查询后手动拼接（如 SeckillVoucherFullModel）

### 全局 ID 生成

雪花算法（SnowflakeIdGenerator）：1位符号 + 41位时间戳 + 5位数据中心 + 5位机器ID + 12位序列号。Worker ID 通过 Redis Lua 原子分配。时钟回拨使用 `Thread.sleep(offset*2)` 保持锁不释放。

---

## 五、核心业务流程

### 秒杀下单全链路

```
用户点击抢购
  ↓
① 令牌前置授权 → 令牌桶限流
  ↓
② doSeckillVoucherV2() Java 侧准备
  ├── queryByVoucherId(): Caffeine → Redis(无锁) → 布隆 → 空值缓存 → MySQL(加锁)
  ├── loadVoucherStock(): 确保 Redis 库存存在
  ├── verifyUserLevel(): 会员等级校验
  └── SnowflakeIdGenerator.nextId(): 生成 orderId + traceId
  ↓
③ Lua 原子执行 (seckillVoucher.lua)
  ├── 校验: 活动时间、状态、库存>0、未重复下单
  ├── incrby stock -1 + sadd userSet + hset traceLog
  └── 返回 {code, beforeQty, deductQty, afterQty}
  ↓
④ Kafka 异步发送 (SeckillVoucherProducer)
  ├── MessageExtend{uuid, key, producerTime, messageBody}
  ├── CompletableFuture 回调
  └── 发送失败 → afterSendFailure → Redis 回滚 (指数退避3次+15%jitter)
  ↓
⑤ Kafka 消费 (SeckillVoucherConsumer)
  ├── beforeConsume: 延迟>10s → 丢弃+回滚
  ├── doConsume → createVoucherOrderV2(@RepeatExecuteLimit 幂等 + @Transactional)
  ├── afterConsumeSuccess: 清理订阅+Top买家统计
  └── afterConsumeFailure: Redis回滚 + 异常重抛 → Kafka 重试
  ↓
⑥ DB 持久化
  ├── 幂等检查 (基于 message.uuid)
  ├── UPDATE stock = stock-1 WHERE stock>0（乐观锁防超卖）
  ├── INSERT voucher_order + router + reconcile_log
  └── 写 Redis 缓存 (TTL 60s)
```

### 一致性保障：三层对账

```
Layer 1 实时补偿: Kafka 发送/消费失败 → 立即 Redis 回滚 (指数退避3次+15%jitter)
Layer 2 定时对账: ReconciliationTaskService
  ├── redisDeductTraceWithoutDbOrder: Redis 有扣减 DB 无订单 → 回滚库存
  └── backfillMissingTraceLogs: DB 有日志 Redis 缺 trace → 回填 Redis
Layer 3 人工兜底: tb_rollback_failure_log + RollbackAlertService 告警
```

### 用户认证流程

```
发送验证码 → 6位随机数 → Redis (TTL 2min)
登录 → 校验验证码（成功后立即删除防重放）→ 创建用户 → JWT → Redis (TTL 10h)
请求拦截 → RefreshTokenInterceptor 刷新 → LoginInterceptor 鉴权
```

---

## 六、Plus 版本改进要点

### 相对于普通版解决的核心问题

| 痛点 | 普通版 | Plus 方案 |
|------|--------|-----------|
| 流量入口无控制 | 无 | 令牌前置授权 + 令牌桶限流，支持 VIP 优先级 |
| 库存超卖 | 乐观锁 | Redis + Lua 原子扣减 + DB `WHERE stock>0` 双重保障 |
| MQ 宕机丢消息 | Redis Stream | Kafka 磁盘持久化 + 多副本 + DLQ |
| MQ 发送/消费失败 | 无处理 | 生产端回调回滚 + 消费端幂等 + 指数退避重试 |
| Redis 与 DB 不一致 | 无 | 三层对账（实时补偿+定时对账+人工兜底） |
| 缓存穿透 | 仅 Redis | 布隆过滤器 + 空值缓存（TTL 2min） |
| 缓存击穿 | 分布式锁 | 双层锁：外层 @ServiceLock(Read) 并发 + 内层互斥锁同步重建 + 双重检查 |
| 缓存雪崩 | 无 | TTL 跟随活动结束时间天然分散 + Caffeine 本地缓存 |
| 分库分表 | 无 | ShardingSphere 2库8分片表 + 雪花算法全局ID + 路由表 |
| 分布式锁 | 单一可重入锁 | 读锁/写锁/公平锁/非公平锁，注解驱动 @ServiceLock |
| 一人一单 | SET + 分布式锁 | Redis SET + @ServiceLock 用户维度锁 + @RepeatExecuteLimit 幂等 |
| 运营能力 | 无 | 每日 Top 买家 (SortedSet) + 到券提醒订阅 + 开抢前2分钟预通知 (延迟队列) |
| 可观测性 | 无 | Micrometer + Prometheus 指标暴露 |

### 缓存体系：四级纵深防御

```
Caffeine 本地 (max=10000, TTL≤60s) → Redis 无锁快速路径 → 布隆过滤器 (防穿透主力)
→ 空值缓存 (防穿透兜底, TTL=2min) → 互斥锁 + 双重检查 + MySQL (同步重建)
```

缓存失效通过 Kafka 广播到所有实例，消费端 @ServiceLock(Write) 写锁与读锁互斥。

### 框架设计亮点

- **模板方法**：MQ 生产/消费框架（AbstractProducerHandler / AbstractConsumerHandler）
- **AOP + SpEL**：@ServiceLock / @RepeatExecuteLimit 注解驱动的动态 Key 解析
- **策略模式 + 条件装配**：限流惩罚策略可插拔（RateLimitPenaltyPolicy）
- **BeanDefinitionRegistryPostProcessor**：声明式布隆过滤器（YAML 配置自动注册 Bean）
- **事件驱动**：延迟队列自动装配（监听 ApplicationStartedEvent）

---

## 七、运行环境

### 中间件

| 中间件 | 版本 | 端口 | 密码 | 启动方式 |
|--------|------|------|------|----------|
| MySQL | 8.0.42 | 3306 | 123456 | 本地服务 |
| Redis | 6.0.8 | 6379 | 123456 | Docker |
| ZooKeeper | latest | 2181 | — | Docker |
| Kafka | wurstmeister/kafka | 9092 | — | Docker |

```bash
docker start zookeeper kafka redis
```

### 服务端口

| 服务 | 端口 | 地址 |
|------|------|------|
| 后端 | 8085 | http://localhost:8085 |
| 前端 | 5173 | http://localhost:5173 |

### IDEA 启动参数

```
-XX:MaxMetaspaceSize=256M -Xmx256M
-Dspring.data.redis.host=127.0.0.1
-Dspring.data.redis.password=123456
-Dspring.kafka.bootstrap-servers=10.21.157.139:9092
-Dprefix.distinction.name=highlight567
```

---

## 八、Bug 修复记录（9 个）

### Bug #1 分布式锁 key 错误，一人一单限制失效 🔴 严重

- **文件**：`VoucherOrderServiceImpl.java:285`
- **问题**：`handleVoucherOrder` 中使用 `voucherOrder.getId()` 获取用户 ID，实际取到的是订单 ID，导致 Redisson 锁的 key 错误
- **影响**：同一用户可以并发下多个秒杀单，"一人一单"机制完全失效
- **修复**：改为 `voucherOrder.getUserId()`

### Bug #2 BlogServiceImpl 空 ids 时 SQL 拼接风险 🟡 中等

- **文件**：`BlogServiceImpl.java:135, 206`
- **问题**：ids 列表为空时 `ORDER BY FIELD(id,)` 产生非法 SQL
- **修复**：拼接前增加空判断，ids 为空时提前返回

### Bug #3 addVoucher ID 生成竞态条件 🔴 严重

- **文件**：`VoucherServiceImpl.java:96-108`
- **问题**：通过 `SELECT MAX(id)+1` 生成新 ID，多线程并发下产生重复 ID 导致主键冲突
- **修复**：改为 `SnowflakeIdGenerator.nextId()` 生成全局唯一 ID

### Bug #4 AopContext.currentProxy() NPE 风险 🟡 中等

- **文件**：`ReconciliationTaskServiceImpl.java`
- **问题**：多处使用 `AopContext.currentProxy()` 触发事务，若 AOP 上下文异常返回 null 导致 NPE
- **修复**：注入自身代理 `@Resource private IReconciliationTaskService self`，替换所有 `AopContext.currentProxy()` 调用

### Bug #5 SeckillVoucherConsumer 日志参数顺序错误 🔴 严重

- **文件**：`SeckillVoucherConsumer.java:140`
- **问题**：日志中 `delayTime` 和 `MESSAGE_DELAY_TIME` 参数位置互换，日志输出误导排查方向
- **修复**：修正参数顺序

### Bug #6 验证码校验后未删除，存在重放攻击风险 🟡 中等

- **文件**：`UserServiceImpl.java:103`
- **问题**：验证码校验成功后没有从 Redis 中删除，TTL 过期前可被重复使用
- **修复**：校验成功后立即执行 `stringRedisTemplate.delete(LOGIN_CODE_KEY + phone)`

### Bug #7 SnowflakeIdGenerator 中 wait() 释放锁风险 🟡 中等

- **文件**：`SnowflakeIdGenerator.java:114`
- **问题**：时钟回拨处理时调用 `wait()`，会释放 `synchronized` 锁，其他线程可趁机进入 `nextId()`，破坏时间戳单调递增
- **修复**：改为 `Thread.sleep(offset << 1)` 保持锁不释放

### Bug #8 handleVoucherOrder 抢锁失败直接丢弃订单 🟡 中等

- **文件**：同 Bug #1
- **问题**：分布式锁获取失败时仅打印日志就 return，订单消息丢失无重试
- **说明**：修复 Bug #1 后风险显著降低（同用户不会并发抢锁），架构上消息层可加重试机制

### Bug #9 LocalDateTime 与 LocalDateTimeUtil 混用 🟢 轻微

- **文件**：`ReconciliationTaskServiceImpl.java:225,230`
- **问题**：混用 `LocalDateTime.now()` 和项目统一的 `LocalDateTimeUtil.now()`
- **修复**：统一使用 `LocalDateTimeUtil.now()`

---

## 九、关键文件索引

| 功能 | 文件路径 |
|------|---------|
| 主应用入口 | `flashgo-core-service/.../FlashGoApplication.java` |
| 秒杀订单服务 | `flashgo-core-service/.../impl/VoucherOrderServiceImpl.java` |
| 秒杀优惠券服务 | `flashgo-core-service/.../impl/SeckillVoucherServiceImpl.java` |
| 优惠券服务 | `flashgo-core-service/.../impl/VoucherServiceImpl.java` |
| 用户服务 | `flashgo-core-service/.../impl/UserServiceImpl.java` |
| 对账服务 | `flashgo-core-service/.../impl/ReconciliationTaskServiceImpl.java` |
| Lua 秒杀扣减脚本 | `flashgo-core-service/src/main/resources/lua/seckillVoucher.lua` |
| Lua 回滚脚本 | `flashgo-core-service/src/main/resources/lua/seckillVoucherRollBack.lua` |
| Kafka 消费者 | `flashgo-core-service/.../kafka/consumer/SeckillVoucherConsumer.java` |
| Kafka 生产者 | `flashgo-core-service/.../kafka/producer/SeckillVoucherProducer.java` |
| Redis 回滚组件 | `flashgo-core-service/.../kafka/redis/RedisVoucherData.java` |
| 缓存失效广播 | `flashgo-core-service/.../cache/SeckillVoucherCacheInvalidationPublisher.java` |
| 缓存失效消费 | `flashgo-core-service/.../kafka/consumer/SeckillVoucherInvalidationConsumer.java` |
| 本地缓存 | `flashgo-core-service/.../cache/SeckillVoucherLocalCache.java` |
| 雪花 ID 生成器 | `flashgo-id-generator-framework/.../SnowflakeIdGenerator.java` |
| 分片配置 | `flashgo-core-service/src/main/resources/shardingsphere.yaml` |
| 应用配置 | `flashgo-core-service/src/main/resources/application.yml` |
| 分布式锁切面 | `flashgo-redisson-framework/.../aspect/ServiceLockAspect.java` |
| 幂等切面 | `flashgo-redisson-framework/.../aspect/RepeatExecuteLimitAspect.java` |
| 限流处理器 | `flashgo-redis-tool-framework/.../execute/RedisRateLimitHandler.java` |
| MQ 生产者模板 | `flashgo-mq-framework/.../AbstractProducerHandler.java` |
| MQ 消费者模板 | `flashgo-mq-framework/.../AbstractConsumerHandler.java` |
| 布隆过滤器封装 | `flashgo-redisson-framework/.../handler/BloomFilterHandler.java` |
| 缓存封装 | `flashgo-redis-tool-framework/.../RedisCacheImpl.java` |
| 数据库 SQL | `sql/` 目录 |
| 前端入口 | `flashgo-vue3/src/main.js` |

---

## 十、启动指南

- [准备项目启动条件](https://javaup.chat/flashgo-plus/startup/prerequisites)
- [安装中间件环境](https://javaup.chat/flashgo-plus/startup/install-middlewares)
- [后端部署启动](https://javaup.chat/flashgo-plus/startup/backend-deploy)
- [前端部署启动](https://javaup.chat/flashgo-plus/startup/frontend-deploy)
- [项目文档和视频目录](https://javaup.chat/flashgo-plus/overview/project-change)

---

## 十一、前端功能

前端基于 Vue 3 + Element Plus + Pinia，包括：

- 抢购优惠券流程优化（页面样式、抢购中提示弹窗、抢购成功/失败反馈）
- 抢购成功后支持取消订单
- 到券提醒：优惠券售罄时可订阅，补货后自动通知
- 订阅通知：确认订阅状态提示
