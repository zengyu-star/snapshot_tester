# OBSA 快照功能与非功能测试策略 (Full Test Strategy)

本文档定义针对 OBSA 插件快照功能的**功能与非功能测试用例集**。每个用例通过"关键词链"表达测试链路，后续将据此生成 Python 测试脚本。

> 核心方法论：遍历**快照命令 × Hadoop 原生命令 × 操作时序**的三维交互矩阵，确保高覆盖深度。

> **用例 ID 规则**：`F{章节号}-{序号}`，如 `F4-03` 表示第 4 章第 3 个用例。新增用例仅需在对应章节追加序号，不影响其他章节。

> **实现状态标记**：✅ 已实现 | ⬚ 未实现

---

## 1. 快照生命周期基础

验证 `allowSnapshot / disallowSnapshot / createSnapshot / deleteSnapshot / renameSnapshot` 五大核心命令的标准路径与边界。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F1-01 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> 快照B -> 越权拦截` | 端到端主流程全链路行为一致性 | `test_f1_lifecycle.py::test_full_lifecycle_parity` |
| F1-02 | ✅ | `赋权 -> 快照A -> renameSnapshot(A→X) -> ls .snapshot -> 验证X可见A不可见` | renameSnapshot 双端一致 | `test_f1_lifecycle.py::test_f1_02_rename_snapshot` |
| F1-03 | ✅ | `赋权 -> 造数 -> 快照A -> deleteSnapshot(A) -> ls .snapshot -> 验证A消失` | deleteSnapshot 双端一致 | `test_f1_lifecycle.py::test_f1_03_delete_snapshot` |
| F1-05 | ✅ | `赋权 -> 造数 -> 快照A -> disallowSnapshot -> 验证拒绝(存在快照不可解权)` | 有快照时 disallowSnapshot 必须被拦截 | `test_f1_lifecycle.py::test_f1_05_disallow_with_snapshots_fails` |
| F1-06 | ✅ | `赋权 -> disallowSnapshot(无快照) -> 验证成功` | 无快照时 disallowSnapshot 正常执行 | `test_f1_lifecycle.py::test_f1_06_disallow_without_snapshots` |
| F1-07 | ✅ | `赋权 -> 重复赋权 -> 验证幂等无副作用` | allowSnapshot 幂等性 | `test_f1_lifecycle.py::test_f1_07_allow_idempotency` |
| F1-08 | ✅ | `直接createSnapshot(未赋权) -> 错误码比对` | 未授权时创建快照错误语义一致 | `test_f1_lifecycle.py::test_f1_08_create_snapshot_unauthorized` |
| F1-09 | ✅ | `赋权 -> 快照A -> 快照B -> deleteSnapshot(A) -> ls .snapshot/B -> 验证B不受影响` | 删除一个快照不影响另一个 | `test_f1_lifecycle.py::test_f1_09_delete_snapshot_isolation` |

---

## 3. 快照 × 文件写入命令交互 (put / touchz / appendToFile / copyFromLocal)

验证快照创建前后，写入类命令对活跃目录的操作不会穿透到历史快照。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F3-01 | ✅ | `赋权 -> 快照A -> put新文件 -> ls .snapshot/A -> 验证新文件不可见` | put 后快照 A 内容不变 | `test_f3_write_isolation.py::test_f3_01_put_not_visible_in_snapshot` |
| F3-02 | ✅ | `赋权 -> 快照A -> touchz新文件 -> ls .snapshot/A -> 验证新文件不可见` | touchz 后快照 A 内容不变 | `test_f3_write_isolation.py::test_f3_02_touchz_not_visible_in_snapshot` |
| F3-03 | ✅ | `赋权 -> 造数 -> 快照A -> put覆盖已有文件 -> cat .snapshot/A/file -> 验证内容是原始值` | put 覆盖不影响快照中的历史版本 | `test_f3_write_isolation.py::test_f3_03_put_overwrite_snapshot_preserves_original` |
| F3-05 | ✅ | `赋权 -> 造数 -> 快照A -> appendToFile -> cat .snapshot/A/file -> 验证内容是追加前原始值` | appendToFile 不影响快照历史版本 | `test_f3_write_isolation.py::test_f3_05_append_snapshot_preserves_original` |
| F3-06 | ✅ | `赋权 -> 造数 -> 快照A -> 多次appendToFile -> 快照B -> cat两个快照中同一文件 -> 验证内容差异` | 多次追加后快照间内容隔离 | `test_f3_write_isolation.py::test_f3_06_multiple_append_isolation` |
| F3-07 | ✅ | `赋权 -> 造数 -> 快照A -> appendToFile -> rm文件 -> cat .snapshot/A/file -> 验证仍可读且是原始值` | 追加后删除，快照仍保留原始版本 | `test_f3_write_isolation.py::test_f3_07_append_then_rm_snapshot_preserved` |

---

## 4. 快照 × truncate 命令交互

HDFS truncate 是对文件做截断操作，验证截小、截大场景与快照的隔离性。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F4-01 | ✅ | `赋权 -> 造数(1KB文件) -> 快照A -> truncate(截小到512B) -> 验证快照中文件大小不变` | truncate 截小后快照内容不受影响 | `test_f4_truncate.py::test_f4_01_truncate_smaller_preserves_snapshot` |
| F4-02 | ✅ | `赋权 -> 造数 -> 快照A -> truncate(截小到0字节) -> cat .snapshot/A/file -> 验证快照中文件内容完整` | truncate 截到空不影响快照版本 | `test_f4_truncate.py::test_f4_02_truncate_zero_preserves_snapshot` |
| F4-04 | ✅ | `赋权 -> 造数 -> 快照A -> truncate .snapshot/A/file -> 拦截断言` | 禁止对快照路径执行 truncate | `test_f4_truncate.py::test_f4_04_truncate_snapshot_path_forbidden` |
| F4-05 | ✅ | `赋权 -> 造数(512B文件) -> 快照A -> truncate(截大到1KB) -> 验证行为(成功或拒绝) -> 双端一致性比对` | truncate 截大场景的双端行为一致性 | `test_f4_truncate.py::test_f4_05_truncate_larger_parity` |
| F4-06 | ✅ | `赋权 -> 造数(512B文件) -> 快照A -> truncate(截大到1KB，若成功) -> cat .snapshot/A/file -> 验证快照内容仍为原始512B` | truncate 截大后快照中文件内容不受影响 | `test_f4_truncate.py::test_f4_06_truncate_larger_snapshot_content_preserved` |

---

## 5. 快照 × rm / mv 命令交互

验证删除和移动操作与快照的隔离性。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F5-01 | ✅ | `赋权 -> 造数 -> 快照A -> rm文件 -> cat .snapshot/A/file -> 验证快照内文件仍可读` | 删除文件后快照仍保留该文件 | `test_f5_rm_mv.py::test_f5_01_rm_file_snapshot_preserves` |
| F5-02 | ✅ | `赋权 -> 造数(含子目录) -> 快照A -> rm -r子目录 -> ls .snapshot/A/子目录 -> 验证子目录仍存在` | 删除子目录后快照仍保留 | `test_f5_rm_mv.py::test_f5_02_rm_subdir_snapshot_preserves` |
| F5-04 | ✅ | `赋权 -> 造数 -> 快照A -> mv文件(目录内重命名) -> ls .snapshot/A -> 验证快照保留旧文件名` | mv 重命名后快照保留原始文件 | `test_f5_rm_mv.py::test_f5_04_mv_rename_snapshot_preserves_old` |
| F5-05 | ✅ | `赋权 -> mv外部文件到快照目录 -> 快照A -> ls .snapshot/A -> 验证新文件在快照中可见` | 快照前 mv 入的文件被快照捕获 | `test_f5_rm_mv.py::test_f5_05_mv_external_to_snapshot_dir` |
| F5-06 | ✅ | `赋权(子目录) -> 快照A -> 父目录 rm -r -> 拦截断言(不允许删除含快照的子目录)` | 父目录删除含快照子目录必须被拦截 | `test_f5_rm_mv.py::test_f5_06_rm_r_parent_containing_snapshot` |

---

## 6. 快照 × cp 命令交互（含快照恢复）

验证从快照路径拷贝恢复数据的能力，以及 cp 到快照路径的拦截。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F6-01 | ✅ | `赋权 -> 造数 -> 快照A -> rm文件 -> cp .snapshot/A/file到活跃目录 -> 验证文件恢复成功` | 从快照路径 cp 恢复单文件 | `test_f6_recovery.py::test_f6_01_recovery_single_file` |
| F6-02 | ✅ | `赋权 -> 造数 -> 快照A -> rm -r子目录 -> cp -r .snapshot/A/子目录到活跃目录 -> ls验证目录树恢复` | 从快照路径 cp 恢复整个子目录树 | `test_f6_recovery.py::test_f6_02_recovery_directory_tree` |
| F6-03 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> cp .snapshot/A/file -> checksum比对 -> 验证与快照前一致` | 恢复文件的内容校验 | `test_f6_recovery.py::test_f6_03_recovery_content_checksum` |
| F6-04 | ✅ | `赋权 -> 造数 -> 快照A -> cp文件到.snapshot/A/ -> 拦截断言` | 禁止向 .snapshot 路径 cp 写入 | `test_f6_recovery.py::test_f6_04_cp_to_snapshot_forbidden` |
| F6-05 | ✅ | `赋权 -> 造数 -> 快照A -> cp .snapshot/A/file到快照目录外 -> 验证目标文件内容正确` | 从快照拷贝到外部路径 | `test_f6_recovery.py::test_f6_05_cp_from_snapshot_to_outside` |

---

## 7. 快照 × chmod / chown / chgrp 权限命令交互

验证权限变更操作与快照的隔离：活跃目录权限变更不影响历史快照中的元数据。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F7-01 | ✅ | `赋权 -> 造数 -> 快照A -> chmod 777 file -> stat .snapshot/A/file -> 验证权限未变` | chmod 不影响快照中文件权限 | `test_f7_permissions.py::test_f7_01_chmod_isolation` |
| F7-02 | ✅ | `赋权 -> 造数 -> 快照A -> chown otheruser file -> stat .snapshot/A/file -> 验证owner未变` | chown 不影响快照中文件 owner | `test_f7_permissions.py::test_f7_02_chown_isolation` |
| F7-03 | ✅ | `赋权 -> 造数 -> 快照A -> chmod .snapshot/A/file -> 拦截断言` | 禁止对快照路径 chmod | `test_f7_permissions.py::test_f7_03_chmod_snapshot_forbidden` |
| F7-04 | ✅ | `赋权 -> 造数 -> 快照A -> chown .snapshot/A/file -> 拦截断言` | 禁止对快照路径 chown | `test_f7_permissions.py::test_f7_04_chown_snapshot_path_forbidden` |
| F7-05 | ✅ | `赋权 -> 造数 -> 快照A -> chgrp newgroup file -> stat .snapshot/A/file -> 验证group未变` | chgrp 不影响快照中文件 group | `test_f7_permissions.py::test_f7_05_chgrp_isolation` |

---

## 8. 快照 × setrep 副本数命令交互

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F8-01 | ✅ | `赋权 -> 造数 -> 快照A -> setrep 5 file -> stat .snapshot/A/file -> 验证副本数不变` | setrep 不影响快照中的副本因子 | `test_f8_replication.py::test_f8_01_setrep_isolation` |
| F8-02 | ✅ | `赋权 -> 造数 -> 快照A -> setrep .snapshot/A/file -> 拦截断言` | 禁止对快照路径 setrep | `test_f8_replication.py::test_f8_02_setrep_snapshot_forbidden` |

---

## 9. 快照 × 只读命令交互 (cat / get / tail / checksum / stat)

验证从 `.snapshot` 路径读取数据的正确性，确保时间旅行语义。

| ID     | 状态 | 测试链路 | 测试目标 | 代码位置 |
|--------|:----:|---------|---------|----|
| F9-01 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> cat .snapshot/A/file -> 验证内容是变异前原始值` | cat 读取快照历史版本 | `test_f9_read_timetravel.py::test_f9_01_cat_snapshot_shows_original` |
| F9-02 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> get .snapshot/A/file到本地 -> 验证下载内容正确` | get 下载快照历史文件 | `test_f9_read_timetravel.py::test_f9_02_get_snapshot_file` |
| F9-03 | ✅ | `赋权 -> 造数 -> checksum(活跃) -> 快照A -> 变异 -> checksum(.snapshot/A/file) -> 验证与快照前一致` | checksum 验证快照时间旅行 | `test_f9_read_timetravel.py::test_f9_03_checksum_time_travel` |
| F9-04 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> stat .snapshot/A/file -> 验证size/modTime是快照时刻值` | stat 元数据时间旅行 | `test_f9_read_timetravel.py::test_f9_04_stat_metadata_time_travel` |
| F9-05 | ✅ | `赋权 -> 造数 -> 快照A -> 变异 -> tail .snapshot/A/file -> 验证输出是变异前内容` | tail 读取快照版本 | `test_f9_read_timetravel.py::test_f9_05_tail_snapshot_file` |

---

## 10. 快照 × ls / count / du 信息查询交互

| ID      | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---------|:----:|---------|---------|----|
| F10-01 | ✅ | `赋权 -> 造数 -> ls -> 快照A -> rm部分文件 -> ls .snapshot/A -> 比对与快照前ls一致` | 快照路径 ls 反映历史状态 | `test_f10_queries.py::test_f10_01_ls_snapshot_consistency` |
| F10-02 | ✅ | `赋权 -> 造数 -> count -> 快照A -> rm文件 -> count .snapshot/A -> 验证文件计数不变` | count 计数时间旅行 | `test_f10_queries.py::test_f10_02_count_isolation` |
| F10-03 | ✅ | `赋权 -> 造数 -> du -> 快照A -> rm文件 -> du .snapshot/A -> 验证空间占用不变` | du 空间统计时间旅行 | `test_f10_queries.py::test_f10_03_du_isolation` |
| F10-04 | ✅ | `赋权 -> 造数 -> 快照A -> 快照B -> ls .snapshot -> 验证能列出 A 和 B 两个快照名` | ls .snapshot 列出全部快照 | `test_f10_queries.py::test_f10_04_ls_all_snapshots` |

---

## 11. 只读隔离性 —— .snapshot 路径写操作全拦截

对 `.snapshot/snapName/` 下的每种写入类命令逐一验证拦截。

| ID      | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---------|:----:|---------|---------|----|
| F11-01 | ✅ | `赋权 -> 造数 -> 快照A -> touchz .snapshot/A/new_file -> 拦截断言` | 禁止 touchz | `test_f11_readonly_block.py::test_f11_01_touchz_blocked` |
| F11-02 | ✅ | `赋权 -> 造数 -> 快照A -> put local_file .snapshot/A/ -> 拦截断言` | 禁止 put | `test_f11_readonly_block.py::test_f11_02_put_blocked` |
| F11-03 | ✅ | `赋权 -> 造数 -> 快照A -> appendToFile .snapshot/A/file -> 拦截断言` | 禁止 appendToFile | `test_f11_readonly_block.py::test_f11_03_append_blocked` |
| F11-04 | ✅ | `赋权 -> 造数 -> 快照A -> rm .snapshot/A/file -> 拦截断言` | 禁止 rm | `test_f11_readonly_block.py::test_f11_04_rm_blocked` |
| F11-05 | ✅ | `赋权 -> 造数 -> 快照A -> mv .snapshot/A/file newpath -> 拦截断言` | 禁止 mv (源端) | `test_f11_readonly_block.py::test_f11_05_mv_source_blocked` |
| F11-06 | ✅ | `赋权 -> 造数 -> 快照A -> mv somefile .snapshot/A/ -> 拦截断言` | 禁止 mv (目标端) | `test_f11_readonly_block.py::test_f11_06_mv_target_blocked` |
| F11-07 | ✅ | `赋权 -> 造数 -> 快照A -> mkdir .snapshot/A/newdir -> 拦截断言` | 禁止 mkdir | `test_f11_readonly_block.py::test_f11_07_mkdir_blocked` |
| F11-08 | ✅ | `赋权 -> 造数 -> 快照A -> rm -r .snapshot/A -> 拦截断言(必须通过deleteSnapshot)` | 禁止 rm -r 快照根 | `test_f11_readonly_block.py::test_f11_08_rm_r_snapshot_root_blocked` |

---

## 12. 多快照时序与交叉操作

验证多个快照之间的时序关系以及跨快照操作的行为。

| ID      | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---------|:----:|---------|---------|----|
| F12-01 | ✅ | `赋权 -> 造数 -> 快照A -> 变异α -> 快照B -> 变异β -> 快照C -> ls各快照 -> 验证内容隔离` | 三快照链各时间点内容正确隔离 | `test_f12_multi_snap_chain.py::test_f12_01_snapshot_chain_isolation` |
| F12-02 | ✅ | `赋权 -> 快照A -> 快照B -> deleteSnapshot(A) -> ls .snapshot/B -> 验证不受A删除影响` | 删除历史快照不影响后续快照 | `test_f12_multi_snap_chain.py::test_f12_02_delete_middle_does_not_break_chain` |

---

## 13. 深度嵌套与目录树

| ID      | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---------|:----:|---------|---------|----|
| F13-01 | ✅ | `赋权(深层路径/d1/.../d10) -> 造数 -> 快照A -> cat .snapshot/A/leaf -> 验证内容` | 10 层以上深度嵌套路径快照行为一致 | `test_f13_deep_nesting.py::test_f13_01_deep_path_snapshot` |
| F13-02 | ✅ | `赋权 -> 造数(3 个并列子目录各含文件) -> 快照A -> 子目录1增 + 子目录2删 + 子目录3改 -> 快照B -> ls 各快照` | 多子目录独立变异的隔离完整性 | `test_f13_deep_nesting.py::test_f13_02_multiple_subdirs_independent_mutations` |
| F13-03 | ✅ | `赋权 -> 造数(宽目录: 50+ 子目录) -> 快照A -> 选择性rm部分子目录 -> 快照B -> ls验证` | 宽目录批量删除的快照准确性 | `test_f13_deep_nesting.py::test_f13_03_wide_directory_batch_rm` |
| F13-04 | ✅ | `赋权 -> 深层mkdir /a/b/c -> 造数 -> 快照A -> mv目录(重命名) -> ls .snapshot/A/原路径 -> 验证存在` | 深层目录重命名后快照一致性 | `test_f13_deep_nesting.py::test_f13_04_deep_rename_interaction` |

---

## 14. 快照 × 快照目录自身操作

验证对已开启快照的目录本身执行操作时的行为约束。

| ID      | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---------|:----:|---------|---------|----|
| F14-01 | ✅ | `赋权 -> 快照A -> ls .snapshot -> 双端一致性比对` | ls 快照根目录一致性 | `test_f14_root_ops.py::test_f14_01_ls_root_parity` |
| F14-02 | ✅ | `赋权 -> 快照A -> touchz .snapshot/A/illegal -> 拦截断言` | 快照路径只读拦截 | `test_f14_root_ops.py::test_f14_02_snapshot_path_as_source_for_put_fails` |
| F14-03 | ✅ | `赋权(父) -> 赋权(子) -> 父快照A -> 子快照B -> 父ls + 子ls -> 验证嵌套独立` | 嵌套 snapshotable 目录的独立性 | `test_f14_root_ops.py::test_f14_03_nested_snapshotable_dirs_independence` |

---

## 15. 快照 × 并发与状态冻结隔离 (Lease / Concat)

验证 OBSA 官方不支持的 Lease 和 Concat 语义在快照场景下的预期失败行为（Expected Failure）或静默降级策略。核心目标是确认不支持的操作不会导致快照数据损坏或产生歧义。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F15-01 | ✅ | `打开文件流写入一半数据(不close) -> 双端打快照 -> close流 -> 读取快照` | 验证不支持 Lease 时的快照边界(预期抛出异常或产生空占位符文件，确保不出现静默错误) | `test_f15_unsupported.py::test_f15_01_snapshot_during_open_stream` |
| F15-02 | ✅ | `造数 -> concat 合并源文件到目标文件 -> 打快照` | 验证 concat 不支持时的拦截一致性或安全降级行为 | `test_f15_unsupported.py::test_f15_02_concat_snapshot_isolation` |

---

## 16. 快照 × 高级元数据隔离 (XAttr / ACL / Storage policy)

验证 OBSA 官方不支持的扩展属性、POSIX ACL 和存储策略。测试目标是确认当用户尝试设置这些元数据并触发快照时，框架能正确捕获并断言 OBSA 的拦截或拒绝服务，确保快照元数据的安全与确定性。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F16-01 | ✅ | `造数 -> setfattr 设置扩展属性 -> 快照 -> getfattr 读取` | 验证不支持 XAttr 时的明确拒绝服务或预期失败(Expected Failure)机制 | `test_f16_metadata.py::test_f16_01_setfattr_interaction` |
| F16-02 | ✅ | `造数 -> setfacl 设置访问控制列表 -> 快照 -> getfacl 读取` | 验证不支持 ACL 时的权限模型降级表现或严格拦截 | `test_f16_metadata.py::test_f16_02_setfacl_interaction` |
| F16-03 | ✅ | `造数 -> 设置 Storage Policy -> 快照` | 验证不支持异构存储策略时的快照兼容性及处理机制 | `test_f16_metadata.py::test_f16_03_storage_policy_interaction` |

---

## 17. 快照 × 拓扑异构拦截 (Symbolic Link)

验证 OBSA 官方不支持的软链接（Symbolic Link）操作。测试目标是确保带有软链接的畸形拓扑在进入快照逻辑前被安全拦截。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F17-01 | ✅ | `造数 -> 创建软链接(hdfs dfs -ln) -> 对父目录打快照` | 验证包含软链接时的快照拦截行为，防止目录树拓扑失效 | `test_f17_symlink.py::test_f17_01_snapshot_with_symlink` |

---

## 18. 快照 × 容量配额交互 (Quota)

验证 OBSA 官方不支持的 Hadoop Quota（配额）机制。测试目标是探查配额耗尽或快照容量计算时，OBSA 的边界行为及其与 HDFS 原生报错的一致性/差异。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F18-01 | ✅ | `设置 NameQuota=5 -> 造数3个 -> 快照 -> 再造3个文件` | 验证不支持 Quota 时，由于快照消耗元数据配额引发的边界表现（预期捕获 Quota 差异） | `test_f18_quota.py::test_f18_01_snapshot_name_quota_consumption` |
| F18-02 | ✅ | `设置 SpaceQuota -> 造大文件 -> 快照 -> append 变异文件` | 验证快照差异数据对物理空间配额的消耗机制及不支持场景下的回退策略 | `test_f18_quota.py::test_f18_02_snapshot_space_quota_consumption` |

---

## 19. 快照 × 回收站机制交互 (Trash Semantics)

验证 HDFS 回收站开启时，删除操作与快照的隔离性，以及 `expunge` 命令对快照物理容量的影响（理论上快照应锁定块，回收站清空不应影响快照）。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F19-01 | ✅ | `赋权 -> 快照A -> rm(不带-skipTrash) -> 验证文件进入.Trash -> ls .snapshot/A` | 验证删除进入回收站后，快照依然能准确引用原始数据块 | `test_f19_trash.py::test_f19_01_rm_to_trash_isolation` |
| F19-02 | ✅ | `赋权 -> 造数 -> rm(进入回收站) -> ls .snapshot -> expunge -> ls .snapshot` | 验证回收站彻底清空后，快照数据的持久性与空间锁定能力 | `test_f19_trash.py::test_f19_02_expunge_interaction` |

---

## 20. 快照 × 边缘文件操作 (moveFromLocal / find / text)

验证 Hadoop HDFS 边缘/不常用命令在快照隐藏路径下的表现。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F20-01 | ✅ | `moveFromLocal 向快照目录写入 -> 快照A -> 验证本地文件删除成功` | 验证 moveFromLocal 事务语义在快照开启目录的正确性 | `test_f20_edge_ops.py::test_f20_01_moveFromLocal_isolation` |
| F20-02 | ✅ | `moveToLocal 从 .snapshot 路径下载 -> 验证拦截报错` | 验证只读路径下 moveToLocal (试图删源) 的安全拦截 | `test_f20_edge_ops.py::test_f20_02_moveToLocal_interception` |
| F20-03 | ✅ | `find 在 .snapshot 目录下搜索 -> 验证搜索结果准确性` | 验证 HDFS find 遍历只读快照路径的兼容性 | `test_f20_edge_ops.py::test_f20_03_find_traversal` |
| F20-04 | ✅ | `造压缩文件 -> 快照A -> hdfs dfs -text .snapshot/A/file.gz -> 验证输出` | 验证 text 命令对快照中压缩元数据的解压读取能力 | `test_f20_edge_ops.py::test_f20_04_text_compression_reader` |

---

## 21. 快照 × 诊断工具交互 (fsck)

验证文件系统级健康检查工具针对快照路径的扫描能力。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| F21-01 | ✅ | `造数 -> 快照A -> hdfs fsck .snapshot/A -> 验证诊断工具兼容性` | 验证 fsck 对只读快照路径及块引用的健康度扫描能力 | `test_f21_fsck.py::test_f21_01_fsck_snapshot_path` |

---

## 22. 性能测试 (Performance)

验证快照在大规模数据情形下的响应耗时以及对主路写入性能的影响。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| P1-01 | ✅ | `批量造数(5/10/20) -> 分别打快照 -> 记录耗时 -> 验证 O(1) 趋势` | 验证快照创建耗时是否随文件数线性增长 | `test_latency_benchmarks.py::test_snapshot_o1_scaling` |
| P1-02 | ✅ | `无快照写基准 -> 开启 5 个快照 -> 变异元数据 -> 再次写 -> 计算 Overhead` | 验证多快照场景下的写入性能损耗 | `test_overhead_benchmarks.py::test_write_overhead_on_snapshotted_dir` |

---

## 23. 可靠性测试 (Reliability)

验证在异常、故障、并发等复杂场景下的系统行为，确保数据最终一致性。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| R1-01 | ✅ | `写入中途 Crash -> 立即打快照 -> 验证快照内文件状态` | 异常掉电/崩溃后的快照一致性 | `test_crash_consistency.py::test_partial_write_snapshot_consistency` |
| R1-02 | ✅ | `append 写入中 kill 客户端 -> 立即打快照 -> 验证重启后状态` | 客户端进程暴力退出后的状态恢复 | `test_crash_consistency.py::test_process_kill_during_append` |
| R1-03 | ✅ | `快照A -> 绕过 API 手动篡改底层对象 -> 快照恢复 -> 验证 Checksum 检错` | 静默损坏检测及快照隔离能力 | `test_data_integrity.py::test_silent_corruption_detection` |
| R1-04 | ✅ | `deleteSnapshot 时注入网络超时 -> 再次删除 -> 验证幂等性与孤兒对象` | 快照异步 GC 过程中的中断韧性 | `test_gc_resilience.py::test_delete_snapshot_interruption_resilience` |
| R1-05 | ✅ | `createSnapshot 时注入 Timeout -> 验证 OBS 侧报错 -> 清理 HDFS 侧幽灵快照` | 创建过程中的网络故障处理 | `test_network_faults.py::test_snapshot_create_with_timeout_sim` |
| R1-06 | ✅ | `注入故障模拟分片 -> 状态裂脑 -> 正常请求尝试修复 -> 验证收敛一致` | 网络恢复后的状态自动修复(Convergence) | `test_network_faults.py::test_recovery_after_simulated_partition` |
| R1-07 | ✅ | `注入 429/503 错误 -> 执行操作 -> 验证重试与报错透传` | 云端流控与服务端错误的弹性响应 | `test_network_faults.py::test_throttling_resilience` |
| R1-08 | ✅ | `大规模 Rename 瞬间触发快照(并发) -> 验证快照元数据一致性` | POSIX 桶重命名原子性与快照的时序保证 | `test_rename_atomicity.py::test_concurrent_rename_and_snapshot` |

---

## 24. 压力测试 (Stress)

验证极限边界条件下系统的稳定性。

| ID | 状态 | 测试链路 | 测试目标 | 代码位置 |
|---|:---:|---|---|---|
| S1-01 | ✅ | `持续造数 + 打快照 (循环 100+) -> 检查链条完整性` | 极长快照链场景下的系统稳定性 | `test_limit_boundaries.py::test_long_snapshot_chain` |
| S1-02 | ✅ | `构造包含 1000+ 文件的超宽目录 -> 打快照 -> 验证成功且耗时在阈值内` | 超大规模目录索引的快照能力 | `test_limit_boundaries.py::test_ultra_wide_directory_snapshot` |

---

## 用例汇总与优先级

### 统计概览

| 指标 | 数量 |
|------|------|
| **有效用例总数** | 77 |
| ✅ 已实现 | 77 |
| ⬚ 未实现 | 0 |

### 优先级矩阵

| 优先级 | 有效用例 ID | 已实现 | 未实现 | 覆盖率 |
|-------|-----------|:------:|:------:|:------:|
| **P0** | F1-01, F3-01~03, F3-05, F5-01, F9-01, F11-01~08, R1-01, R1-08 | 16 | 0 | **100%** |
| **P1** | F1-02, F1-03, F1-05~F1-09, F3-06~07, F4-01~06, F5-02~06, F7-01~05, F8-01~02, F16-01, F16-02, F19-01, F19-02, P1-02, R1-03, R1-05, S1-01 | 32 | 0 | **100%** |
| **P2** | F6-01~05, F9-02~05, F10-01~04, F12-01~02, F13-01~04, F14-01~03, F15-01~02, F16-03, F17-01, F18-01~02, F20-01~04, F21-01, P1-01, R1-02, R1-04, R1-06, R1-07, S1-02 | 29 | 0 | **100%** |

> **已实现 100% HDFS 命令集与快照场景的交互测试。**
