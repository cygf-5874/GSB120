/**
 * 慢消费者场景的最小复现。
 *
 * 一路写、一路只放行一块给消费者，看看缓冲的水位到底受不受控、
 * 派发出去的块数对不对、drain 到底有没有出现。
 *
 * 不依赖墙钟：消费者的节奏由手动的 Promise 控制。
 */
import { StreamBuf } from './src/streambuf.js';

const HWM = 4;
const TOTAL = 20;

const settle = () => new Promise((resolve) => setImmediate(resolve));

const gates = [];
const consumed = [];
const drains = [];
const buf = new StreamBuf({
  highWaterMark: HWM,
  onData: (c) => {
    consumed.push(c[0]);
    return new Promise((resolve) => gates.push(resolve));
  },
});
buf.on('drain', () => drains.push(buf.bufferedLength));

let peak = 0;
const note = () => {
  if (buf.bufferedLength > peak) peak = buf.bufferedLength;
};

for (let i = 0; i < TOTAL; i += 1) {
  const ok = buf.write(Buffer.from([i]));
  note();
  if (!ok) {
    // 到达高水位：慢消费者只放行一块，等下游缓过来
    const gate = gates.shift();
    if (gate !== undefined) gate();
    await settle();
  }
}
// 收工：把还压在缓冲里的块全部放行
for (let n = 0; n < TOTAL * 2 && buf.bufferedLength > 0; n += 1) {
  const gate = gates.shift();
  if (gate === undefined) break;
  gate();
  await settle();
}
note();

process.stdout.write(`写入 ${TOTAL} 块 1 字节，高水位 ${HWM}\n`);
process.stdout.write(`派发给消费者的块数 = ${consumed.length}\n`);
process.stdout.write(`bufferedLength 峰值 = ${peak}（背压上限 ${HWM * 4}）\n`);
process.stdout.write(`全部放行之后的 bufferedLength = ${buf.bufferedLength}\n`);
process.stdout.write(`drain 次数 = ${drains.length}\n`);

const problems = [];
if (consumed.length !== TOTAL) {
  problems.push(`有 ${TOTAL - consumed.length} 块没有交给消费者`);
}
if (buf.bufferedLength !== 0) {
  problems.push(`消费完之后 bufferedLength 应为 0，实际 ${buf.bufferedLength}`);
}
if (peak >= HWM * 4) {
  problems.push(`bufferedLength 峰值 ${peak} 没有被背压压到 ${HWM * 4} 以下`);
}
if (drains.length === 0) {
  problems.push('整个过程一次 drain 都没有派发');
}

if (problems.length > 0) {
  process.stdout.write('复现到问题：\n');
  for (const p of problems) process.stdout.write(`- ${p}\n`);
  process.exit(1);
}
process.stdout.write('没有复现到问题。\n');
process.exit(0);
