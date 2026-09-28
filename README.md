# streambuf

带**背压**的字节缓冲。Node.js 22、ESM，只用 `node:` 内置模块，**零第三方依赖**。

- 入口：`src/streambuf.js`（导出 `StreamBuf`、`DEFAULT_HIGH_WATER_MARK`）
- 既有用例：`test/streambuf.test.js`（12 个，当前全绿）
- 复现脚本：`repro.mjs`（慢消费者场景）
- 固定验收程序：`check/check.mjs`（**不要修改**）
- 自检：`bash scripts/check.sh`

```bash
node --test                    # 既有用例（仓库根目录裸跑）
node check/check.mjs           # 跑全部场景
node check/check.mjs -list     # 只列出 组/名
node check/check.mjs --only memory
node repro.mjs                 # 慢消费者复现
bash scripts/check.sh --only drain
```

---

## 用法

```js
import { StreamBuf } from './src/streambuf.js';

const buf = new StreamBuf({ highWaterMark: 16, onData: (chunk) => { /* 消费一块 */ } });

if (!buf.write(chunk)) {
  await once(buf, 'drain');    // 到达高水位，等下游缓过来
}
buf.end();                     // 冲刷完缓冲之后派发 'finish'
```

- `onData(chunk)` 是**唯一**的消费入口；它可以返回一个 Promise（表示「这块还没消费完」），
  缓冲会等它落定之后才继续下一块；返回其它值（含 `undefined`）表示同步消费完成。
- `StreamBuf` 继承 `node:events` 的 `EventEmitter`，会派发 `drain` / `finish` / `error`。
- `bufferedLength` 是「已写入但消费者还没消费完」的字节数。

## 对外保证（8 条）

1. `write(chunk) -> boolean`：返回 `false` 表示**本次写入到达了高水位**、调用方应暂停。
2. 一旦 `write` 返回过 `false`（即进入背压态），此后 `bufferedLength` **首次降到**
   `highWaterMark` 以下时**必须**派发 `drain` 事件，且**恰一次**；`drain` 派发的那一刻
   `bufferedLength` 必须 `< highWaterMark`。
3. **数据不丢不重**：所有写入的 chunk 按序、完整地交给 `onData`，
   交出去的字节总数等于写入的字节总数之和。
4. `end()` 之后 `write` 抛错；`end()` 必须把**已缓冲**的数据全部冲刷完（消费者处理完）
   再派发 `finish`。
5. 慢消费者不得导致内存无界增长：`bufferedLength` 在无界生产 + 慢消费下必须被**背压**压制
   （`check` 观测的峰值 `< highWaterMark * 4`）。
6. `error` 之后不再派发 `data`；错误对象**原样**传出，不吞不换。
7. 空 chunk（长度 0）合法，不计入 `bufferedLength`，原样派发。
8. 同一实例多轮 `write` / `drain` 循环后状态不残留（第 N 轮开始时 `bufferedLength` 从 0 起算）。

### 精确定义（保证判据确定、与实现无关）

- **高水位**：`highWaterMark` 是构造参数，缺省 `16`（字节），必须为正有限数。
- **到达高水位**：某次 `write` 把 chunk 放进缓冲之后 `bufferedLength >= highWaterMark`。
- **背压态**：某次 `write` 返回值是 `false` 之后，缓冲进入背压态；`drain` 派发一次之后解除。
  没有进入背压态时不得派发 `drain`。
- **冲刷**：把队首的 chunk 交给 `onData`；**消费者处理完**（同步返回或返回的 Promise 落定）
  之后这一块才算被消费，才从缓冲里去掉。消费者抛错 / Promise 拒绝时，这一块**必须仍在缓
  冲里**，不得丢弃。
- **`end()`**：可以在一部分 chunk 还没被消费完时调用；它只负责「写完了」，
  剩下的事交给冲刷——缓冲全部被消费完之后才派发 `finish`。重复调用 `end()` 是幂等的。
- **`error`**：消费者抛错（同步抛或返回的 Promise 被拒绝）时，把**同一个错误对象**通过
  `error` 事件传出，之后不再派发任何 `data`；此后 `write` 返回 `false`。
- **空 chunk**：`Buffer.alloc(0)` 也是一块，按顺序派发，但不增加 `bufferedLength`。
- 结果不得依赖墙钟、随机源；慢消费者一律用**可控的异步节奏**（手动的 Promise），不要用 `sleep`。

## 导出面

- `src/streambuf.js`：`StreamBuf`（`write` / `end` / `bufferedLength` / `highWaterMark`，
  事件 `drain` / `finish` / `error`）、`DEFAULT_HIGH_WATER_MARK`。

## 约束

- 只用 `node:` 内置模块；`package.json` 里**不许出现 `dependencies`**。
- 不改 `check/`、不改 `test/streambuf.test.js`；可以新增文件，
  但 `src/streambuf.js` 的导出名与签名不许改。
- 不许用 `Date.now()`、`Math.random()`、`setTimeout` 凑时间。
