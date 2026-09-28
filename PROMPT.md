下游慢下来之后，上游还在往里塞，内存就顶不住。streambuf 是 Node.js 22 的
背压缓冲，只用 `node:` 内置模块（无第三方依赖），构建不需要额外步骤，
自检走 `scripts/check.sh`（`check/` 是固定验收程序，别改），既有用例走 `node --test`。

既有用例只用同步可读流、一次只写一块，所以都是绿的 —— 问题的位置得你自己找。
`repro.mjs` 是慢消费者场景的复现脚本。

任务：按 README「对外保证」的 8 条把缺陷修掉，让固定件全过。修完既有用例仍须全绿。

验收：
- node --test 全绿；
- node check/check.mjs 退出码 0，8 个场景全过（drain 2 + integrity 2 + memory 2 + end 1 + error 1）。

约束：
1. 不改 `check/`；可以新增模块。
2. 对外导出名与签名不许改；`test/streambuf.test.js` 里的既有用例一条都不许删或改。
3. 只用 `node:` 内置模块，`package.json` 里不许出现 `dependencies`。
4. 慢消费者场景不要靠 sleep 凑时间，用可控的异步节奏。
