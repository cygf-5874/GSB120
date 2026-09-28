#!/usr/bin/env bash
# streambuf 固定件入口：等价于在仓库根目录执行 node check/check.mjs
# 用法：bash scripts/check.sh [-list | --only <组名>]
exec node check/check.mjs "$@"
