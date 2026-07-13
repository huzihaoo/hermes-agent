# G1Q3-RCA 报告 v2 设计:因果文本叙事 + HTML 报告体验升级

状态:设计定稿待实施(2026-06-12)。实施方:Codex(VM 仓库 `/home/mini/data3/yj-evaluation-server`,分支 `g1q3-rca`)。
关联文档:`g1q3-rca-auto-pipeline-design.md`(管线)、`g1q3-rca-ops-runbook.md`(运维)。
本文档只新增/修改 S7 报告层(`report_builder.py` / `output_writer.py` / `rca_receipt.py` / `rca/arbiter.py`)及一个新发布步骤 S7b,不触碰 S1-S6 管线契约。

> **输出路径勘误(2026-07-11,优先于本文旧口径):** 本文关于把新 case 发布到 perception-test-team NAS cases root、从该 root 提供 HTTP/CIFS 链接及清理其治理残留的内容，均是 2026-06-12 下载时代的 historical/superseded 方案，不得用于当前生产发布。新生产 candidate 的每单派生缓存、报告与交付物只写 `/mnt/tmp/<submission_key>/`，对外路径为 `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<submission_key>/`；链接必须来自当前 delivery manifest/contract。感知测试 NAS 上既有 case 只作历史资产，当前切换不迁移、不回刷、不清理。

## 0. 现状基线(实施前必读)

- 报告为单文件 `index.html` + `report_data.json`,模板内联在 `output_writer.py` 的 `HTML` 字符串(约 line 14-341)。
- 布局:三栏 workbench——视频列(video + BEV canvas)/ 当前值列 / 曲线面板列;卡尺为 `syncCursor={t}`,拖动曲线时更新视频 `currentTime` 与 BEV 重绘。
- 曲线为 canvas 自绘(`plot-fallback`),无坐标轴刻度优化、无 tooltip、无缩放。
- BEV 用 lanebev 多项式系数绘制——**注意 T5a 标定结论:本平台 lanebev curve 系数恒为 0,车道线几何唯一可信源是 det_points**;现 BEV 实现部分基于 coeff,属已知缺陷,v2 必须改为 det_points 源。
- 因果链:`rca_receipt.causal_chain` 仅有 `domain`/`pattern` 结构字段;HTML 顶部 `globalEvidence` 把 evaluator 行平铺,无"现象→证据→结论→责任方"叙事。
- 链接策略(runbook 固化):飞书内只发 `http://192.168.26.174:18081/...`,`file://hfs...` 在飞书客户端不可点。CIFS 路径目前是 pnc NAS(`//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/...`)。
- 红线(不可违反):归因永远 need_review 不自动确认;mcap 工具只用预编译 `--skip-build`;飞书写操作只有 comment add。

## 1. 痛点 → 方案映射

| # | 痛点 | 方案条目 |
|---|------|---------|
| P1(历史,已废弃) | 有 `//hfs.minieye.tech/department-perception_test_team/` NAS 权限的同学可直接点开 HTML | §2 记录 2026-06-12 historical 方案；当前生产输出路径见顶部勘误 |
| P2 | 视频/俯视图/数据/其他要素全部基于卡尺对齐 | §3 统一时间轴契约 + 卡尺总线 |
| P3 | 坐标 plot 可读性 | §4 plot 渲染升级 |
| P4 | 布局要素保持、可读性更好 | §5 布局 v2 |
| P5 | 归因排查视角(责任 owner)可提升点 | §6 因果叙事文本 + owner 排查视图 |

## 2. 历史 P1:感知测试 NAS 产物方案(superseded)

> 本节仅保留历史设计依据。新 case 不再写 perception-test-team NAS cases root；当前唯一生产输出根为 `/mnt/tmp/<submission_key>/`，历史 case 原地保留且不迁移。

> 2026-06-12 真实案例核对修正:cases root **本来就在**感知测试 NAS(`config.py`:`DEFAULT_OUTPUT_ROOT=/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA`,VM 挂载点 `/mnt/minieye/pdcl/department/perception_test_team`,`CIFS_OUTPUT_ROOT` 已是目标路径)。原设计的 S7b"发布步骤"取消,P1 的真实缺口是下面三项。

### 2.1 自包含化(现状已基本成立,补校验)
- 实测 7017699515:`index.html` 1.6MB,`__REPORT_DATA__` 内联(scrubbed 子集),唯一外部引用是相对路径 `assets/media/video.mp4`——SMB 直开已可用。
- 补 `html_validation` 检查项 `self_contained: true`(扫描 html 不得出现 `http(s)://`、`file://`、绝对路径资源引用),防回归。
- 若 §4 引入图表库,必须 vendor 内联进 html(推荐 uPlot,~45KB min),禁止 CDN。
- 补 scrub:`report_data.json` 与内联数据中的 VM 绝对路径(实测 `media.source_video_path`、evidence `source` 字段泄露 `/mnt/minieye/...`)统一替换为相对路径或脱敏标记,纳入 `_public_report_for_html` 的 scrub 列表。

### 2.2 产物分层(替代原 S7b,本节为 P1 主体)
实测单 case 目录 ~6.7G,其中原始数据 uuid 目录 3.9G + `_dt_work` 中间产物 2.6G 直接留在共享 NAS;交付物本体(html+json+assets)仅 ~226M。改为三层:
- **交付层(留 NAS case 目录)**:`index.html`、`report_data.json`、`assets/plots/`、`assets/media/video.mp4`、receipt/manifest 类小 json。
- **中间层(不落共享 NAS)**:`_dt_work/`、原始数据 uuid 目录、`assets/media/video_source.h265`(216M assets 里的大头)——管线工作目录改在 VM 本地 `/mnt/tmp/<task_slug>/`,S7 完成后仅把交付层产物写入 NAS case 目录;或保持现工作流但 S7 末尾做清理搬迁(取实施侵入小者,推荐前者:工作目录与交付目录分离)。
- **追溯层**:中间产物按现有磁盘预算策略在 VM 本地保留/清理;`download_manifest.json` 已记录数据来源可重取,不需要 NAS 留原始数据。
- **当前替代契约:** 上述 NAS 分层仅作历史记录。新 case 的全部派生产物固定在 `/mnt/tmp/<submission_key>/`，不得写感知测试 NAS cases root；既有历史 case 不回刷、不迁移，也不在本次切换中清理 `.bak/probe_backup` 等历史资产。

### 2.3 历史链接策略(双链接,superseded)
- 本小节以下 HTTP/CIFS cases-root 方案不得用于新生产 case；当前链接必须由 `/mnt/tmp/<submission_key>/` 下的 delivery manifest/contract 证明，不能从历史 cases root 或文件存在性推导。
- 群回读与飞书评论模板升级为两行:
  1. **HTTP 链接**(飞书可点):`http://192.168.26.174:18081/...`,指向 cases root 下对应 case;
  2. **CIFS 路径**(文本展示,供有 NAS 权限同学在资源管理器/Finder 粘贴直开):`\\hfs.minieye.tech\department-perception_test_team\G1Q3_RCA\cases\<case_key>\index.html`(同时给 `smb://` 形式一份,Mac 用)。
- 明确预期:CIFS 路径在飞书内不可点是平台限制,交付口径是"粘贴到文件管理器双击 index.html 即开",报告自包含保证这条路径可用。
- HTTP 服务确认项:`g1q3-rca-report-http.service`(18081)当前服务根是否覆盖感知测试 NAS 的 cases root,实施时确认;不覆盖则加软链。
- **路径长度/编码风险**:case 目录名含长中文标题(实测 >60 字符),Windows 上 `\\UNC` + 长中文路径可能超 MAX_PATH 或编码异常。新 case 的 `case_key` 改为 `<issue_id>_<domain>` 短形式,中文标题只进页面 `<title>` 与索引页显示列,不进目录名;历史目录不改。验收必须含一台 Windows 实机直开测试。

## 3. P2:统一时间轴契约 + 卡尺总线

### 3.1 时间轴契约
- 报告内唯一权威时间轴:`t_abs`(绝对秒,基准 `alignment.base_start_abs`)。`report_data.json` 新增顶层 `timebase` 对象:
  ```json
  {"base_start_abs": ..., "issue_t": ..., "focus_window": {"start": ..., "end": ...},
   "video": {"start_abs": ..., "duration_s": ..., "fps": ...},
   "bev": {"frame_index": [[t_abs, frame_id], ...]}}
  ```
- 视频偏移 `video.start_abs` 必须来自对齐 provenance(index/json 时戳),禁止用"视频时长反推"的启发式兜底;无可信偏移时报告页明示"视频未对齐,仅供参考",且 `html_validation` 记 warning `video_unaligned`,该 case 不得给 `html_delivery_ready`。
- **focus_window 修复(实测缺口)**:7017699515 的 `case_meta.focus_window` 四字段全 null 但报告仍判 `html_delivery_ready`(score 100)——构建侧需保证 focus_window 从 evaluator 窗口/issue_time 推导兜底成功,推导失败计 warning;`html_validation` 增加 `has_focus_window` 检查项。

### 3.2 卡尺总线(前端)
- 重构为发布-订阅:`Caliper.set(t)` → 通知所有注册渲染器;渲染器清单(全部必须订阅,这是验收点):
  1. 视频:`currentTime = t - video.start_abs`(clamp 到 [0, duration]);
  2. 俯视图 BEV:按 `bev.frame_index` 最近邻取帧重绘——**数据源改为 det_points 逐帧流**(lanebev coeff 恒 0,弃用),目标框/CIPV 同帧绘制;
  3. 全部曲线面板:卡尺竖线 + 当前值高亮;
  4. 当前值表:每信号取 ≤t 最近样本,显示值与 `Δt`(样本距卡尺秒差,>0.5s 标灰提示陈旧);
  5. 因果叙事(§6)中的证据时间戳:点击即 `Caliper.set(t)` 反向驱动。
- 双向:拖曲线、拖视频进度条、点叙事时间戳,三个入口都走同一 `Caliper.set`,杜绝各自为政。
- 卡尺读数条固定吸顶显示:`t_abs / 相对issue Δt / frame_id`。

## 4. P3:坐标 plot 可读性

- 渲染引擎:vendor 内联 **uPlot**(满足自包含;若 Codex 评估内联体积或接入成本过高,可保留 canvas 自绘但必须实现下列全部特性,uPlot 为推荐路径)。
- 必备特性清单(验收逐条对):
  1. x 轴统一为 `t_abs` 相对 issue 的秒数(`issue_t` 处为 0),刻度带单位;y 轴自适应范围 + 10% padding,刻度人类可读(避免 `0.30000000004`);
  2. 信号名带单位标注(panel 元数据补 `unit` 字段,builder 侧从信号字典映射,未知留空不猜);
  3. hover tooltip:时间 + 各序列值;
  4. issue 时刻红色竖线 + evaluator 命中窗口浅色阴影(数据来自 causal_chain 证据窗口);
  5. 长序列 LTTB 降采样(>2000 点),缩放后按窗口重采;
  6. 滚轮/框选缩放 + 双击复位,x 轴缩放在多面板间联动;
  7. 色板换 Okabe-Ito 色盲安全色,线宽 ≥1.5px,图例可点击隐藏序列;
  8. **懒渲染(实测必需)**:真实 case panels 达 340 个,必须 IntersectionObserver 视口内才建图、出视口销毁/暂停,卡尺更新只重绘可见面板;首屏只展开命中归因的面板。

## 5. P4:布局 v2(要素保持,重排提升可读性)

要素清单不增不减:案件信息、结论/证据卡、视频、俯视图、当前值、信号选择器、曲线面板、(新增的)因果叙事区。

- **顶部带(全宽,常驻)**:案件信息(case_id/function/alignment/window/双链接)压缩为一行 chips + 结论摘要一句话 + 卡尺读数条(吸顶)。
- **左栏(媒体)**:视频在上、BEV 在下(保持);BEV 增加视距/缩放控制和帧号角标。
- **中栏改为"归因叙事栏"**(替换原"当前值"独占一栏):§6 的因果链卡片 + owner 排查清单;当前值表收纳为叙事栏底部可折叠区(或入曲线 tooltip,实施取其一,默认折叠区)。
- **右栏(曲线)**:按域分组(perception/planning/control/vehicle/…)可折叠分区,命中归因的面板自动置顶展开并打"证据"角标,其余默认折叠;信号选择器保留在分组头。
- 响应式断点保留现有 1100px 单列降级。
- 字号/对比度:正文 ≥12px,muted 色对比度 ≥4.5:1(现 `#8a8f98` on 深底偏低,微调)。

## 6. P5:因果叙事文本 + owner 排查视图

### 6.1 因果叙事层(后端,基于既有结构扩展,不重建)

> 真实案例修正:`report_data.json` 顶层已有 `causal_chain{domain,pattern,hypotheses[],fusion}`(hypothesis 含 claim/confidence/supporting_evidence,证据带 `abs_t`)与独立 `responsibility{candidates,most_likely_module,confidence,missing_evidence,...}` 对象。**禁止另起炉灶**,叙事层在两者之上生成:

- 新增 `causal_chain.narrative[]`(由 hypotheses + evaluator 结果模板拼装):

```json
[
  {"step": 1, "role": "现象", "text": "...", "t": <abs_t|null>, "panel_ids": [...]},
  {"step": 2, "role": "证据", "text": "trigger_early 命中:AEBReq 上升沿 t=...,TTC/gate 风险上下文不足", "t": ..., "panel_ids": [...], "hypothesis_id": "H1"},
  {"step": 3, "role": "因果判断", "text": "...", "confidence": 0.7},
  {"step": 4, "role": "排除项", "text": "refuted 检查 X/Y,排除…"}
]
```

- 文本由 evaluator 注册处的 `narrative_template` 拼装,**不引入 LLM**,可复算可审计;`summary.short_conclusion` 同步改为从 narrative step1+3 生成的 case 特定一句话(实测现为通用套话"当前页展示…",无信息量,替换)。
- **owner 双轨区分**(实测发现的关键点):现 `responsibility` 的 owner 候选 `evidence_role=metadata_only`,来自 `g1q3_rca_benchmark_patterns.json` 的 benchmark 元数据(如"刘培瑞"),不是证据推导。叙事层必须显式区分并在 UI 标注来源:
  - `benchmark_owner`(元数据先验,"该模式历史上由 X 负责"),
  - `evidence_owner_domain`(由命中检查的 owner_domain 路由表推导,`config/owner_routing.yaml`,检查→责任域,19 条目人工指定,`unassigned` 兜底)。
  - 两者一致时增强置信展示;冲突时并列展示且标"待人工裁决",不自动合并。
- 红线不变:narrative 措辞一律"候选/建议核查",`status` 不因有 narrative 而升级,确认权在人。

### 6.2 owner 排查视图(前端,中栏)
- 因果链时间线卡片:按 step 渲染,带 confidence 徽章;每条证据时间戳可点(驱动卡尺,见 §3.2);
- "责任域排查入口"卡:owner_domain + 建议动作 + 证据锚点按钮("跳到 t=…");多候选时按 arbiter 排序列出;
- "已排除项"折叠卡:refuted 检查及理由——这是给 owner 减负的关键(明确不用查什么);
- 字段缺口/对齐降级(near_match)在叙事区顶部黄条提示,写明对结论可信度的影响;
- 人审反馈:页内不做写操作(报告是静态文件),提供"复制反馈模板"按钮,生成含 case_id/候选归因/同意-否决格式的文本,人贴回飞书评论——后续 M2 人审反馈闭环复用该格式。

### 6.3 其他 owner 视角提升点(纳入本期)
- 全局索引页(cases root `index.html`)增加列:owner_domain、confidence、alignment 档位、报告分,支持按 owner 过滤——owner 只看自己域的待排查 case;
- 报告页标题与 `<title>` 带 case_id + 功能域 + 候选归因短语,方便浏览器多 tab 检索;
- `report_data.json` 中 narrative/owner_route 完整保留,供后续指标周报(T9)统计各 owner 域归因分布。

## 7. WBS(建议实施顺序)

| 任务 | 内容 | 依赖 |
|---|---|---|
| W1 | 自包含校验 + 绝对路径 scrub + `/mnt/tmp/<submission_key>/` 单任务输出隔离 + manifest-backed 链接；不得写 perception-test-team cases root | 无 |
| W2 | timebase 契约 + focus_window 修复 + 卡尺总线重构 + BEV 改 det_points 源(§3) | 无 |
| W3 | plot 引擎升级 + 340 面板懒渲染(§4) | W2(共用卡尺总线) |
| W4 | narrative 层(基于既有 hypotheses/responsibility)+ owner 双轨路由 + short_conclusion 替换(§6.1) | 无,可与 W2 并行 |
| W5 | 布局 v2 + owner 排查视图 + 索引页升级(§5、§6.2、§6.3) | W2-W4 |
| W6 | 回归:5 个 M1 case + 7017699515 全量重建报告 + html_validation 扩展项 + `/mnt/tmp/<submission_key>/` 路径/依赖闭合校验；不得改动或清理历史 cases root | W1-W5 |

## 8. 验收标准(逐条可检)

1. **P1(当前替代验收)**:任一新建 case 的派生缓存、HTML、JSON、assets、receipt 与 delivery artifact 均位于 `/mnt/tmp/<submission_key>/`，且 delivery manifest/contract 证明依赖闭合、URL 可读和 `self_contained` 校验通过；perception-test-team NAS cases root 无新增或修改，历史 case 不迁移、不回刷、不清理。
2. **P2**:拖动卡尺到任意 t,视频帧(±1 帧)、BEV 帧(最近邻)、所有曲线竖线、当前值表、叙事证据高亮显示同一时刻;点击叙事时间戳卡尺跳转一致;BEV 车道线来自 det_points(用 7015689036 对照视频肉眼核验车道几何不再是直线假象)。
3. **P3**:§4 特性清单 7 条逐条演示通过;以 7015849914(LCC/near_match)曲线密集 case 验证降采样与缩放。
4. **P4**:要素清单(§5 首行)无缺失;命中归因面板自动置顶;1100px 以下单列可用。
5. **P5**:7015689036 报告 narrative 呈现"现象→证据→因果判断→排除项"四段,owner 路由(evidence_owner_domain)指向感知-mono3d,与既有人工归因一致;7015895406 指向控制(自车震荡);7017699515 的 benchmark_owner(刘培瑞/metadata_only)与 evidence_owner_domain 并列展示且来源标注清晰;`summary.short_conclusion` 为 case 特定结论而非套话;索引页可按 owner 过滤;所有 narrative 措辞无"确认/定责"字样,receipt status 语义不变。
6. **回归**:5/5 M1 case + 7017699515 重建后 `html_validation` 不低于原档位(exact_match 仍 delivery_ready);340 面板 case 首屏可交互(命中面板秒级渲染,滚动流畅);单测全绿。

## 10. 真实案例审查记录(7017699515,2026-06-12)

本设计 v2 修订依据,实施时可作回归对照:

| 发现 | 影响的设计节 |
|---|---|
| cases root 已在感知测试 NAS(config.py DEFAULT_OUTPUT_ROOT),CIFS_OUTPUT_ROOT 即目标路径 | historical observation only；§2 输出方案已被 `/mnt/tmp/<submission_key>/` 当前契约取代 |
| 单 case 6.7G:uuid 原始数据 3.9G + `_dt_work` 2.6G + `video_source.h265` 留在共享 NAS;交付物仅 ~226M | §2.2 |
| html 自包含已成立(REPORT_DATA 内联,唯一外链是相对 video.mp4);但 json/内联数据泄露 `/mnt/minieye/...` 绝对路径 | §2.1 scrub |
| 目录名 = issue_id + 长中文标题,Windows UNC 长路径/编码风险 | §2.3 case_key 短化 |
| `causal_chain` 已有 hypotheses[](claim/confidence/abs_t 证据)、独立 `responsibility`(benchmark_owner=刘培瑞,evidence_role=metadata_only) | §6.1 在既有结构上扩展、owner 双轨 |
| `summary.short_conclusion` 为通用套话,无 case 信息 | §6.1 替换 |
| `focus_window` 四字段全 null、`alignment_status` None,仍 delivery_ready/100 | §3.1 修复 + 校验项 |
| panels=340,现实现一次性渲染 | §4 懒渲染 |
| cases root 存在 `.bak`/`probe_backup` 治理残留 | historical observation only；当前切换不清理、不迁移 |

## 9. 执行约定(给 Codex)

- 仓库/分支:VM `/home/mini/data3/yj-evaluation-server`,分支 `g1q3-rca`;每个 W 任务独立 commit,commit message 带 `W<N>` 标号;单测随改随加。
- 不可触碰:S1-S6 编排契约、blocker 三类语义、归因 need_review 红线、mcap 预编译约束、飞书写操作范围(仅 comment add)。
- 感知测试 NAS 挂载点 `/mnt/minieye/pdcl/department/perception_test_team` 仅为历史资料/数据源位置，不是新生产 case 输出根；不得在当前实现中向其写入、迁移或清理 case。
- 阈值/样式拿不准时:plot 视觉细节 Codex 自行裁量,**timebase 语义、owner 路由表内容、链接模板措辞**三类改动若需偏离本设计,先留档说明再实施,主脑 gate 复核。
- 验收方式:W6 完成后产出自验报告(逐条对 §8,附 case 链接与截图说明),落 `outputs/` 留档;主脑按 §8 抽查后才记 done。
