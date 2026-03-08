# OBSA 快照功能测试策略 (Functional Test Strategy)

本文档定义针对 OBSA 插件快照功能的**功能测试用例集**。每个用例通过"关键词链"表达测试链路，后续将据此生成 Python 测试脚本。

> 核心方法论：遍历**快照命令 × Hadoop 原生命令 × 操作时序**的三维交互矩阵，确保高覆盖深度。

> **用例 ID 规则**：`F{章节号}-{序号}`，如 `F4-03` 表示第 4 章第 3 个用例。新增用例仅需在对应章节追加序号，不影响其他章节。

---

## 1. 快照生命周期基础

验证 `allowSnapshot / disallowSnapshot / createSnapshot / deleteSnapshot / renameSnapshot` 五大核心命令的标准路径与边界。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F1-01 | `赋权 -> 造数 -> 快照A -> 变异 -> 快照B -> 差异比对 -> 越权拦截` | 端到端主流程全链路行为一致性 |
| F1-02 | `赋权 -> 快照A -> renameSnapshot(A→X) -> ls .snapshot -> 验证X可见A不可见` | renameSnapshot 双端一致 |
| F1-03 | `赋权 -> 造数 -> 快照A -> deleteSnapshot(A) -> ls .snapshot -> 验证A消失` | deleteSnapshot 双端一致 |
| F1-04 | `赋权(空目录) -> 快照A -> snapshotDiff(A, .) -> 验证空diff` | 空目录快照 diff 输出为空 |
| F1-05 | `赋权 -> 造数 -> 快照A -> disallowSnapshot -> 验证拒绝(存在快照不可解权)` | 有快照时 disallowSnapshot 必须被拦截 |
| F1-06 | `赋权 -> disallowSnapshot(无快照) -> 验证成功` | 无快照时 disallowSnapshot 正常执行 |
| F1-07 | `赋权 -> 重复赋权 -> 验证幂等无副作用` | allowSnapshot 幂等性 |
| F1-08 | `直接createSnapshot(未赋权) -> 错误码比对` | 未授权时创建快照错误语义一致 |
| F1-09 | `赋权 -> 快照A -> 快照B -> deleteSnapshot(A) -> ls .snapshot/B -> 验证B不受影响` | 删除一个快照不影响另一个 |

---

## 2. snapshotDiff 差异标记全覆盖

逐一验证 snapshotDiff 报告中 `+`(新增)、`-`(删除)、`M`(修改)、`R`(重命名) 每种标记的准确性。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F2-01 | `赋权 -> 快照A -> 新增文件 -> 快照B -> snapshotDiff(A,B) -> 验证出现"+"标记` | 新增文件的 + 标记一致 |
| F2-02 | `赋权 -> 造数 -> 快照A -> rm文件 -> 快照B -> snapshotDiff(A,B) -> 验证出现"-"标记` | 删除文件的 - 标记一致 |
| F2-03 | `赋权 -> 造数 -> 快照A -> appendToFile -> 快照B -> snapshotDiff(A,B) -> 验证出现"M"标记` | 追加写入的 M 标记一致 |
| F2-04 | `赋权 -> 造数 -> 快照A -> mv文件(目录内重命名) -> 快照B -> snapshotDiff(A,B) -> 验证出现"R"标记` | 重命名文件的 R 标记一致 |
| F2-05 | `赋权 -> 快照A -> mkdir新子目录 -> 快照B -> snapshotDiff(A,B) -> 验证出现"+目录"标记` | 新增子目录的 + 标记 |
| F2-06 | `赋权 -> 造数(含子目录) -> 快照A -> rm -r子目录 -> 快照B -> snapshotDiff(A,B) -> 验证出现"-目录"标记` | 删除子目录的 - 标记 |
| F2-07 | `赋权 -> 造数 -> 快照A -> 混合变异(增+删+改+重命名) -> 快照B -> snapshotDiff(A,B) -> 验证多标记共存` | 混合变异的差异报告完整性 |
| F2-08 | `赋权 -> 造数 -> 快照A -> 无变异 -> 快照B -> snapshotDiff(A,B) -> 验证diff为空` | 无变异时 diff 输出空 |
| F2-09 | `赋权 -> 造数 -> 快照A -> truncate文件 -> 快照B -> snapshotDiff(A,B) -> 验证出现"M"标记` | truncate 产生 M 标记 |

---

## 3. 快照 × 文件写入命令交互 (put / touchz / appendToFile / copyFromLocal)

验证快照创建前后，写入类命令对活跃目录的操作不会穿透到历史快照。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F3-01 | `赋权 -> 快照A -> put新文件 -> ls .snapshot/A -> 验证新文件不可见` | put 后快照 A 内容不变 |
| F3-02 | `赋权 -> 快照A -> touchz新文件 -> ls .snapshot/A -> 验证新文件不可见` | touchz 后快照 A 内容不变 |
| F3-03 | `赋权 -> 造数 -> 快照A -> put覆盖已有文件 -> cat .snapshot/A/file -> 验证内容是原始值` | put 覆盖不影响快照中的历史版本 |
| F3-04 | `赋权 -> 快照A -> copyFromLocal -> 快照B -> snapshotDiff(A,B) -> 验证"+"标记` | copyFromLocal 与 diff 交互 |
| F3-05 | `赋权 -> 造数 -> 快照A -> appendToFile -> cat .snapshot/A/file -> 验证内容是追加前原始值` | appendToFile 不影响快照历史版本 |
| F3-06 | `赋权 -> 造数 -> 快照A -> 多次appendToFile -> 快照B -> cat两个快照中同一文件 -> 验证内容差异` | 多次追加后快照间内容隔离 |
| F3-07 | `赋权 -> 造数 -> 快照A -> appendToFile -> rm文件 -> cat .snapshot/A/file -> 验证仍可读且是原始值` | 追加后删除，快照仍保留原始版本 |

---

## 4. 快照 × truncate 命令交互

HDFS truncate 是对文件做截断操作，验证截小、截大、组合时序三种场景与快照的隔离性和差异记录。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F4-01 | `赋权 -> 造数(1KB文件) -> 快照A -> truncate(截小到512B) -> 快照B -> snapshotDiff(A,B) -> 验证M标记` | truncate 截小后 diff 记录正确 |
| F4-02 | `赋权 -> 造数 -> 快照A -> truncate(截小到0字节) -> cat .snapshot/A/file -> 验证快照中文件内容完整` | truncate 截到空不影响快照版本 |
| F4-03 | `赋权 -> 造数 -> 快照A -> truncate(截小) -> appendToFile恢复内容 -> 快照B -> snapshotDiff(A,B) -> 验证M标记` | truncate截小 + append 组合变异后差异记录 |
| F4-04 | `赋权 -> 造数 -> 快照A -> truncate .snapshot/A/file -> 拦截断言` | 禁止对快照路径执行 truncate |
| F4-05 | `赋权 -> 造数(512B文件) -> 快照A -> truncate(截大到1KB) -> 验证行为(成功或拒绝) -> 双端一致性比对` | truncate 截大场景的双端行为一致性（HDFS 原生 truncate 不支持截大，需验证 OBSA 侧错误语义一致） |
| F4-06 | `赋权 -> 造数(512B文件) -> 快照A -> truncate(截大到1KB，若成功) -> cat .snapshot/A/file -> 验证快照内容仍为原始512B` | truncate 截大后快照中文件内容不受影响 |
| F4-07 | `赋权 -> 造数(512B文件) -> 快照A -> truncate(截大到1KB，若成功) -> 快照B -> snapshotDiff(A,B) -> 验证M标记` | truncate 截大后差异标记正确记录 |
| F4-08 | `赋权 -> 造数 -> 快照A -> truncate(截小到一半) -> truncate(截大到原始大小) -> 快照B -> cat .snapshot/A/file vs cat活跃文件 -> 验证内容差异 + diff(A,B)有M标记` | truncate 截小再截大的组合时序：快照保留原始内容，活跃文件截大部分被补零 |

---

## 5. 快照 × rm / mv 命令交互

验证删除和移动操作与快照的隔离性和差异语义。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F5-01 | `赋权 -> 造数 -> 快照A -> rm文件 -> cat .snapshot/A/file -> 验证快照内文件仍可读` | 删除文件后快照仍保留该文件 |
| F5-02 | `赋权 -> 造数(含子目录) -> 快照A -> rm -r子目录 -> ls .snapshot/A/子目录 -> 验证子目录仍存在` | 删除子目录后快照仍保留 |
| F5-03 | `赋权 -> 造数 -> 快照A -> mv文件到快照目录外 -> 快照B -> snapshotDiff(A,B) -> 验证"-"标记` | mv 出目录等价于删除 |
| F5-04 | `赋权 -> 造数 -> 快照A -> mv文件(目录内重命名) -> 快照B -> snapshotDiff(A,B) -> 验证"R"标记` | mv 目录内重命名产生 R 标记 |
| F5-05 | `赋权 -> mv外部文件到快照目录 -> 快照A -> ls .snapshot/A -> 验证新文件在快照中可见` | 快照前 mv 入的文件被快照捕获 |
| F5-06 | `赋权(子目录) -> 快照A -> 父目录 rm -r -> 拦截断言(不允许删除含快照的子目录)` | 父目录删除含快照子目录必须被拦截 |

---

## 6. 快照 × cp 命令交互（含快照恢复）

验证从快照路径拷贝恢复数据的能力，以及 cp 到快照路径的拦截。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F6-01 | `赋权 -> 造数 -> 快照A -> rm文件 -> cp .snapshot/A/file到活跃目录 -> 验证文件恢复成功` | 从快照路径 cp 恢复单文件 |
| F6-02 | `赋权 -> 造数 -> 快照A -> rm -r子目录 -> cp -r .snapshot/A/子目录到活跃目录 -> ls验证目录树恢复` | 从快照路径 cp 恢复整个子目录树 |
| F6-03 | `赋权 -> 造数 -> 快照A -> 变异 -> cp .snapshot/A/file -> checksum比对 -> 验证与快照前一致` | 恢复文件的内容校验 |
| F6-04 | `赋权 -> 造数 -> 快照A -> cp文件到.snapshot/A/ -> 拦截断言` | 禁止向 .snapshot 路径 cp 写入 |
| F6-05 | `赋权 -> 造数 -> 快照A -> cp .snapshot/A/file到快照目录外 -> 验证目标文件内容正确` | 从快照拷贝到外部路径 |

---

## 7. 快照 × chmod / chown / chgrp 权限命令交互

验证权限变更操作与快照的隔离：活跃目录权限变更不影响历史快照中的元数据。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F7-01 | `赋权 -> 造数 -> 快照A -> chmod 777 file -> stat .snapshot/A/file -> 验证权限未变` | chmod 不影响快照中文件权限 |
| F7-02 | `赋权 -> 造数 -> 快照A -> chown otheruser file -> stat .snapshot/A/file -> 验证owner未变` | chown 不影响快照中文件 owner |
| F7-03 | `赋权 -> 造数 -> 快照A -> chmod .snapshot/A/file -> 拦截断言` | 禁止对快照路径 chmod |
| F7-04 | `赋权 -> 造数 -> 快照A -> chown .snapshot/A/file -> 拦截断言` | 禁止对快照路径 chown |
| F7-05 | `赋权 -> 造数 -> 快照A -> chgrp newgroup file -> stat .snapshot/A/file -> 验证group未变` | chgrp 不影响快照中文件 group |

---

## 8. 快照 × setrep 副本数命令交互

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F8-01 | `赋权 -> 造数 -> 快照A -> setrep 5 file -> stat .snapshot/A/file -> 验证副本数不变` | setrep 不影响快照中的副本因子 |
| F8-02 | `赋权 -> 造数 -> 快照A -> setrep .snapshot/A/file -> 拦截断言` | 禁止对快照路径 setrep |

---

## 9. 快照 × 只读命令交互 (cat / get / tail / checksum / stat)

验证从 `.snapshot` 路径读取数据的正确性，确保时间旅行语义。

| ID     | 测试链路 | 测试目标 |
|--------|---------|---------|
| F9-01 | `赋权 -> 造数 -> 快照A -> 变异 -> cat .snapshot/A/file -> 验证内容是变异前原始值` | cat 读取快照历史版本 |
| F9-02 | `赋权 -> 造数 -> 快照A -> 变异 -> get .snapshot/A/file到本地 -> 验证下载内容正确` | get 下载快照历史文件 |
| F9-03 | `赋权 -> 造数 -> checksum(活跃) -> 快照A -> 变异 -> checksum(.snapshot/A/file) -> 验证与快照前一致` | checksum 验证快照时间旅行 |
| F9-04 | `赋权 -> 造数 -> 快照A -> 变异 -> stat .snapshot/A/file -> 验证size/modTime是快照时刻值` | stat 元数据时间旅行 |
| F9-05 | `赋权 -> 造数 -> 快照A -> 变异 -> tail .snapshot/A/file -> 验证输出是变异前内容` | tail 读取快照版本 |

---

## 10. 快照 × ls / count / du 信息查询交互

| ID      | 测试链路 | 测试目标 |
|---------|---------|---------|
| F10-01 | `赋权 -> 造数 -> ls -> 快照A -> rm部分文件 -> ls .snapshot/A -> 比对与快照前ls一致` | 快照路径 ls 反映历史状态 |
| F10-02 | `赋权 -> 造数 -> count -> 快照A -> rm文件 -> count .snapshot/A -> 验证文件计数不变` | count 计数时间旅行 |
| F10-03 | `赋权 -> 造数 -> du -> 快照A -> rm文件 -> du .snapshot/A -> 验证空间占用不变` | du 空间统计时间旅行 |
| F10-04 | `赋权 -> 造数 -> 快照A -> 快照B -> ls .snapshot -> 验证能列出 A 和 B 两个快照名` | ls .snapshot 列出全部快照 |

---

## 11. 只读隔离性 —— .snapshot 路径写操作全拦截

对 `.snapshot/snapName/` 下的每种写入类命令逐一验证拦截。

| ID      | 测试链路 | 测试目标 |
|---------|---------|---------|
| F11-01 | `赋权 -> 造数 -> 快照A -> touchz .snapshot/A/new_file -> 拦截断言` | 禁止 touchz |
| F11-02 | `赋权 -> 造数 -> 快照A -> put local_file .snapshot/A/ -> 拦截断言` | 禁止 put |
| F11-03 | `赋权 -> 造数 -> 快照A -> appendToFile .snapshot/A/file -> 拦截断言` | 禁止 appendToFile |
| F11-04 | `赋权 -> 造数 -> 快照A -> rm .snapshot/A/file -> 拦截断言` | 禁止 rm |
| F11-05 | `赋权 -> 造数 -> 快照A -> mv .snapshot/A/file newpath -> 拦截断言` | 禁止 mv (源端) |
| F11-06 | `赋权 -> 造数 -> 快照A -> mv somefile .snapshot/A/ -> 拦截断言` | 禁止 mv (目标端) |
| F11-07 | `赋权 -> 造数 -> 快照A -> mkdir .snapshot/A/newdir -> 拦截断言` | 禁止 mkdir |
| F11-08 | `赋权 -> 造数 -> 快照A -> rm -r .snapshot/A -> 拦截断言(必须通过deleteSnapshot)` | 禁止 rm -r 快照根 |

---

## 12. 多快照时序与交叉操作

验证多个快照之间的时序关系、差异累积，以及跨快照操作的行为。

| ID      | 测试链路 | 测试目标 |
|---------|---------|---------|
| F12-01 | `赋权 -> 造数 -> 快照A -> 变异α -> 快照B -> 变异β -> 快照C -> diff(A,B) + diff(B,C) + diff(A,C) -> 验证差异累积` | 三快照差异链的累积正确性 |
| F12-02 | `赋权 -> 快照A -> 快照B -> deleteSnapshot(A) -> diff(B,.) -> 验证不受A删除影响` | 删除历史快照不影响后续快照 diff |
| F12-03 | `赋权 -> 造数 -> 快照A -> renameSnapshot(A→X) -> 变异 -> 快照B -> diff(X,B) -> 验证重命名后diff正常` | rename 后 diff 引用新名仍正常 |
| F12-04 | `赋权 -> 造数 -> 快照A -> rm所有文件 -> 快照B -> 重新造数 -> 快照C -> diff(A,B)全删 + diff(B,C)全增` | 清空→重建的极端差异场景 |
| F12-05 | `赋权 -> 造数 -> 快照A -> 变异 -> 快照B -> deleteSnapshot(B) -> 重建同名快照B' -> diff(A,B') -> 验证是新快照内容` | 删除后重建同名快照的差异正确性 |
| F12-06 | `赋权 -> 造数 -> 快照A -> mv文件(重命名) -> 快照B -> appendToFile(mv后文件) -> 快照C -> diff(A,B)(R标记) + diff(B,C)(M标记)` | 重命名 + 追加的跨快照时序 |
| F12-07 | `赋权 -> 造数 -> 快照A -> truncate -> 快照B -> rm -> 快照C -> diff(A,B)(M标记) + diff(B,C)(-标记) + diff(A,C)(-标记)` | truncate + rm 的跨快照差异累积 |

---

## 13. 深度嵌套与目录树

| ID      | 测试链路 | 测试目标 |
|---------|---------|---------|
| F13-01 | `赋权(深层路径/a/b/c/d) -> 造数 -> 快照A -> 叶子目录变异 -> 快照B -> diff(A,B) -> 验证嵌套标记完整` | 4 层以上深度嵌套路径快照行为一致 |
| F13-02 | `赋权 -> 造数(3 个并列子目录各含文件) -> 快照A -> 子目录1增 + 子目录2删 + 子目录3改 -> 快照B -> diff(A,B)` | 多子目录独立变异的 diff 标记完整性 |
| F13-03 | `赋权 -> 造数(宽目录: 50+ 子目录) -> 快照A -> 选择性rm部分子目录 -> 快照B -> diff(A,B) -> 验证"-"标记数正确` | 宽目录批量删除的差异报告准确性 |
| F13-04 | `赋权 -> 深层mkdir /a/b/c -> 造数 -> 快照A -> rm -r /a/b/c -> mkdir /a/b/c(同名重建) -> 新造数 -> 快照B -> diff(A,B)` | 深层目录删除并重建同路径后的差异语义 |

---

## 14. 快照 × 快照目录自身操作

验证对已开启快照的目录本身执行操作时的行为约束。

| ID      | 测试链路 | 测试目标 |
|---------|---------|---------|
| F14-01 | `赋权 -> 快照A -> mv快照目录(重命名) -> 验证快照仍可通过新路径/.snapshot/A访问` | 重命名快照根目录后快照仍可用 |
| F14-02 | `赋权 -> 快照A -> rm -r快照目录 -> 拦截断言(不允许删除含快照的目录)` | 有快照时禁止删除快照根目录 |
| F14-03 | `赋权(父) -> 赋权(子) -> 父快照A -> 子快照B -> 父diff + 子diff -> 验证嵌套独立` | 嵌套 snapshotable 目录的独立性 |

---

## 用例汇总与优先级

共计 **72** 个功能测试用例。

| 优先级 | 用例 ID | 理由 |
|-------|--------|------|
| P0 | F1-01, F2-01~F2-09, F3-01~F3-03, F3-05, F5-01, F9-01, F11-01~F11-08 | 核心快照语义 + 主要安全边界 |
| P1 | F1-02~F1-09, F3-04, F3-06~F3-07, F4-01~F4-08, F5-02~F5-06, F7-01~F7-05, F8-01~F8-02 | 基础 API 全覆盖 + 元数据隔离 |
| P2 | F6-01~F6-05, F9-02~F9-05, F10-01~F10-04, F12-01~F12-07, F13-01~F13-04, F14-01~F14-03 | 恢复场景 + 多快照时序 + 嵌套 |
