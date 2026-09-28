#!/usr/bin/env node
/**
 * streambuf 固定验收程序（固定件）。
 *
 * 用法（在仓库根目录）：
 *   node check/check.mjs            跑全部场景
 *   node check/check.mjs -list      只列出 `组/名`
 *   node check/check.mjs --only <组>  只跑某一组
 *
 * 输出：逐场景 `PASS <组>/<名>` 或 `FAIL <组>/<名>  期望=… 实际=…`，
 * 结尾 `结果：通过 x/N`；全过 exit 0，否则 exit 1；失败不早退。
 *
 * 判据只描述**对外可见性质**（write 的返回值、drain 的次数与水位、交出去的字节、
 * bufferedLength 的观测值、finish/error 的时序与内容），不引用任何内部实现细节。
 * 慢消费者一律用手动的 Promise 控制节奏，不依赖墙钟；本文件不含任何缓冲实现。
 */
import { StreamBuf } from '../src/streambuf.js';

/* ------------------------------------------------------------------ 工具 */

const fail = (msg) => {
  throw new Error(msg);
};

const eq = (actual, expected, label) => {
  if (actual !== expected) {
    fail(`${label}：期望 ${String(expected)}，实际 ${String(actual)}`);
  }
};

/** 让挂起的微任务与 setImmediate 回调跑完（一个宏任务轮次，不依赖墙钟）。 */
const settle = () => new Promise((resolve) => setImmediate(resolve));

const bytes = (...values) => Buffer.from(values);

/** 逐个放行消费者的 gate，直到队列里没有待放行的为止（有界）。 */
async function releaseAll(gates, limit) {
  for (let n = 0; n < limit && gates.length > 0; n += 1) {
    const gate = gates.shift();
    gate();
    await settle();
  }
}

/* ---------------------------------------------------------------- 场景表 */

const scenarios = [
  {
    group: 'drain',
    name: '到达高水位触发一次',
    expect:
      '同步消费者下，每把一个 chunk 写到刚好等于 highWaterMark 就返回 false，并且各补一次 drain（3 次写入 → 恰好 3 次 drain，每次 drain 时 bufferedLength < 高水位）',
    run() {
      const HWM = 4;
      const seen = [];
      const drains = [];
      const buf = new StreamBuf({ highWaterMark: HWM, onData: (c) => seen.push(c[0]) });
      buf.on('drain', () => drains.push(buf.bufferedLength));

      const returns = [];
      for (let i = 0; i < 3; i += 1) returns.push(buf.write(Buffer.alloc(HWM, i)));
      if (returns.some((v) => v !== false)) {
        fail(`写到 ${HWM} 字节时 write 必须返回 false，实际 [${returns.join(', ')}]`);
      }
      eq(seen.length, 3, '被消费的块数');
      eq(drains.length, 3, '进入过 3 次背压态，必须恰好补 3 次 drain');
      for (const level of drains) {
        if (!(level < HWM)) fail(`drain 时的 bufferedLength=${level} 必须 < ${HWM}`);
      }
    },
  },
  {
    group: 'drain',
    name: 'drain时长度低于水位',
    expect:
      '慢消费者下水位从 4 降到 3（仍 > 0）时就必须派发 drain，且 drain 时 bufferedLength=3 < 高水位；进入背压态后水位下降前不得派发',
    async run() {
      const HWM = 4;
      const gates = [];
      const drains = [];
      const buf = new StreamBuf({
        highWaterMark: HWM,
        onData: () => new Promise((resolve) => gates.push(resolve)),
      });
      buf.on('drain', () => drains.push(buf.bufferedLength));

      const returns = [];
      for (let i = 0; i < HWM; i += 1) returns.push(buf.write(bytes(i)));
      await settle();
      eq(returns[HWM - 1], false, `第 ${HWM} 块写入必须返回 false`);
      eq(drains.length, 0, '进入背压态之后、水位下降之前不得派发 drain');

      const gate = gates.shift();
      if (gate === undefined) fail('第一块没有被交给消费者');
      gate();
      await settle();

      eq(buf.bufferedLength, HWM - 1, '放行一块之后的 bufferedLength');
      eq(drains.length, 1, '水位降到高水位以下时必须派发一次 drain');
      if (!(drains[0] < HWM)) fail(`drain 时的 bufferedLength=${drains[0]} 必须 < ${HWM}`);
      if (!(drains[0] > 0)) {
        fail(`drain 必须在缓冲还没排空时就派发，实际 bufferedLength=${drains[0]}`);
      }
    },
  },

  {
    group: 'integrity',
    name: '不丢不重',
    expect: '24 块交给慢消费者，交出去的块数与字节流必须与写入的完全一致（不丢、不重、不改内容）',
    async run() {
      const gates = [];
      const seen = [];
      const written = [];
      const buf = new StreamBuf({
        highWaterMark: 8,
        onData: (c) => {
          seen.push(Buffer.from(c));
          return new Promise((resolve) => gates.push(resolve));
        },
      });

      for (let i = 0; i < 24; i += 1) {
        const chunk = Buffer.from(`#${i};`, 'latin1');
        written.push(chunk);
        buf.write(chunk);
      }
      await releaseAll(gates, 40);

      eq(seen.length, written.length, '被消费的块数');
      if (!Buffer.concat(seen).equals(Buffer.concat(written))) {
        fail(
          `交出去的 ${Buffer.concat(seen).length} 字节与写入的 ${Buffer.concat(written).length} 字节不一致`,
        );
      }
    },
  },
  {
    group: 'integrity',
    name: '顺序正确',
    expect: '混合大小的块（含一块比高水位还大）按写入顺序、整块交给消费者，块边界不被拆开或合并',
    async run() {
      const sizes = [1, 1, 9, 1, 2, 1, 5, 1];
      const gates = [];
      const seen = [];
      const buf = new StreamBuf({
        highWaterMark: 6,
        onData: (c) => {
          seen.push([c.length, c[0]]);
          return new Promise((resolve) => gates.push(resolve));
        },
      });

      const written = [];
      sizes.forEach((size, tag) => {
        const chunk = Buffer.alloc(size, tag);
        written.push([size, tag]);
        buf.write(chunk);
      });
      await releaseAll(gates, 20);

      const got = JSON.stringify(seen);
      const want = JSON.stringify(written);
      if (got !== want) fail(`交出去的块（长度,首字节）期望 ${want}，实际 ${got}`);
    },
  },

  {
    group: 'memory',
    name: '无界生产下峰值受背压压制',
    expect:
      '生产端一到高水位就停、等 drain 再继续；64 块必须在有界轮数内生产完并全部交给消费者，过程中 bufferedLength 峰值 < 高水位 * 4（=32）',
    async run() {
      const HWM = 8;
      const TOTAL = 64;
      const gates = [];
      const seen = [];
      const buf = new StreamBuf({
        highWaterMark: HWM,
        onData: (c) => {
          seen.push(c[0]);
          return new Promise((resolve) => gates.push(resolve));
        },
      });

      let produced = 0;
      let peak = 0;
      for (let step = 0; step < 400 && produced < TOTAL; step += 1) {
        while (produced < TOTAL) {
          const ok = buf.write(bytes(produced & 0xff));
          produced += 1;
          if (buf.bufferedLength > peak) peak = buf.bufferedLength;
          if (!ok) break; // 到达高水位，生产端暂停
        }
        const gate = gates.shift();
        if (gate !== undefined) gate();
        await settle();
      }

      if (!(peak < HWM * 4)) {
        fail(`bufferedLength 峰值 ${peak} 必须 < ${HWM * 4}`);
      }
      eq(produced, TOTAL, `有界轮数内必须生产完 ${TOTAL} 块`);

      // 生产收工之后，把还压在缓冲里的块全部放行，consumer 必须一块不落地收齐。
      await releaseAll(gates, TOTAL + 20);
      eq(seen.length, TOTAL, '被消费的块数');
    },
  },
  {
    group: 'memory',
    name: '多轮循环状态不残留',
    expect: '同一实例跑 3 轮「写 6 块 → 全部消费」，每轮开始前 bufferedLength 必须是 0，每轮 6 块都要被消费',
    async run() {
      const HWM = 4;
      const gates = [];
      const seen = [];
      const buf = new StreamBuf({
        highWaterMark: HWM,
        onData: (c) => {
          seen.push(c[0]);
          return new Promise((resolve) => gates.push(resolve));
        },
      });

      for (let round = 0; round < 3; round += 1) {
        eq(buf.bufferedLength, 0, `第 ${round + 1} 轮开始前的 bufferedLength`);
        const before = seen.length;
        for (let i = 0; i < 6; i += 1) buf.write(bytes(round * 16 + i));
        await releaseAll(gates, 12);
        eq(seen.length - before, 6, `第 ${round + 1} 轮被消费的块数`);
      }
    },
  },

  {
    group: 'end',
    name: '冲刷完再finish',
    expect:
      '还有缓冲没被消费完时调用 end()，此刻不得派发 finish；等三块全部被消费完之后必须恰好派发一次 finish',
    async run() {
      const gates = [];
      const seen = [];
      const events = [];
      const buf = new StreamBuf({
        highWaterMark: 4,
        onData: (c) => {
          seen.push(c[0]);
          return new Promise((resolve) => gates.push(resolve));
        },
      });
      buf.on('finish', () => events.push('finish'));

      for (let i = 0; i < 3; i += 1) buf.write(bytes(i));
      buf.end();
      await settle();

      eq(events.length, 0, '缓冲还没冲刷完时不得派发 finish');
      eq(seen.length, 1, '此刻只应有第一块被交给消费者');

      await releaseAll(gates, 10);
      eq(seen.length, 3, '三块都必须被消费');
      eq(events.length, 1, '全部冲刷完之后必须派发一次 finish');
    },
  },

  {
    group: 'error',
    name: 'error后不再派发data',
    expect:
      '消费者在第二块同步抛错：error 必须原样传出该错误对象；此后不得再派发任何 data；未被消费的两块必须仍留在缓冲里（bufferedLength=2）',
    async run() {
      const boom = new Error('consumer boom');
      const gates = [];
      const seen = [];
      const errors = [];
      let failedOnce = false;

      const buf = new StreamBuf({
        highWaterMark: 100,
        onData: (c) => {
          seen.push(c[0]);
          if (c[0] === 2 && !failedOnce) {
            failedOnce = true;
            throw boom;
          }
          return new Promise((resolve) => gates.push(resolve));
        },
      });
      buf.on('error', (e) => errors.push(e));

      buf.write(bytes(1));
      buf.write(bytes(2));
      buf.write(bytes(3));
      await settle();
      eq(seen.length, 1, '一开始只应有第一块被交给消费者');

      const gate = gates.shift();
      if (gate === undefined) fail('第一块没有被交给消费者');
      gate();
      await settle();

      eq(errors.length, 1, '消费者抛错必须派发一次 error');
      if (errors[0] !== boom) fail('error 必须原样传出同一个错误对象');
      eq(seen.join(','), '1,2', 'error 之后不得再派发任何 data');
      eq(buf.bufferedLength, 2, '未被消费的两块必须仍留在缓冲里');
    },
  },
];

/* ---------------------------------------------------------------- 运行器 */

const argv = process.argv.slice(2);

if (argv.includes('-list')) {
  for (const s of scenarios) process.stdout.write(`${s.group}/${s.name}\n`);
  process.exit(0);
}

let only = null;
const onlyAt = argv.indexOf('--only');
if (onlyAt >= 0) {
  only = argv[onlyAt + 1];
  if (only === undefined) {
    process.stdout.write(
      '用法：node check/check.mjs --only <组名>（组名：drain / integrity / memory / end / error）\n',
    );
    process.exit(1);
  }
}

const selected = only === null ? scenarios.slice() : scenarios.filter((s) => s.group === only);

if (selected.length === 0) {
  process.stdout.write(`结果：通过 0/0（没有匹配的场景：--only ${String(only)}）\n`);
  process.exit(1);
}

let passed = 0;
for (const s of selected) {
  const id = `${s.group}/${s.name}`;
  try {
    await s.run();
    passed += 1;
    process.stdout.write(`PASS ${id}\n`);
  } catch (e) {
    const actual = e instanceof Error ? e.message : String(e);
    process.stdout.write(`FAIL ${id}  期望=${s.expect} 实际=${actual}\n`);
  }
}

process.stdout.write(`结果：通过 ${passed}/${selected.length}\n`);
process.exit(passed === selected.length ? 0 : 1);
