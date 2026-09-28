import { test } from 'node:test';
import assert from 'node:assert/strict';

import { StreamBuf } from '../src/streambuf.js';

test('write：未到高水位时返回 true，且 chunk 立刻被派出', () => {
  const seen = [];
  const buf = new StreamBuf({ highWaterMark: 16, onData: (c) => seen.push(c.toString('latin1')) });
  const ok = buf.write(Buffer.from('hello', 'latin1'));
  assert.equal(ok, true);
  assert.deepEqual(seen, ['hello']);
});

test('write：达到高水位时返回 false，消费后派发一次 drain', () => {
  const seen = [];
  const drains = [];
  const buf = new StreamBuf({ highWaterMark: 4, onData: (c) => seen.push(c.toString('latin1')) });
  buf.on('drain', () => drains.push(buf.bufferedLength));

  const ok = buf.write(Buffer.from('abcd', 'latin1'));
  assert.equal(ok, false);
  assert.deepEqual(seen, ['abcd']);
  assert.deepEqual(drains, [0]);
  assert.equal(buf.bufferedLength, 0);
});

test('write：多块按写入顺序派发（同步消费者）', () => {
  const seen = [];
  const buf = new StreamBuf({ highWaterMark: 64, onData: (c) => seen.push(c[0]) });
  buf.write(Buffer.from([1]));
  buf.write(Buffer.from([2]));
  buf.write(Buffer.from([3]));
  assert.deepEqual(seen, [1, 2, 3]);
});

test('write：派发出去的全部字节与写入的全部字节相同', () => {
  const written = [Buffer.from('ab', 'latin1'), Buffer.from('cdef', 'latin1'), Buffer.from('g', 'latin1')];
  const got = [];
  const buf = new StreamBuf({ highWaterMark: 64, onData: (c) => got.push(Buffer.from(c)) });
  for (const w of written) buf.write(w);
  assert.deepEqual(Buffer.concat(got), Buffer.concat(written));
});

test('bufferedLength：同步消费之后回到 0', () => {
  const buf = new StreamBuf({ highWaterMark: 64, onData: () => {} });
  buf.write(Buffer.from('abc', 'latin1'));
  assert.equal(buf.bufferedLength, 0);
});

test('bufferedLength：消费者尚未消费完时等于已写入的字节数', () => {
  const gates = [];
  const buf = new StreamBuf({
    highWaterMark: 64,
    onData: () => new Promise((resolve) => gates.push(resolve)),
  });
  buf.write(Buffer.from('abc', 'latin1'));
  buf.write(Buffer.from('de', 'latin1'));
  assert.equal(buf.bufferedLength, 5);
  assert.equal(gates.length, 1);
});

test('end：之后再 write 抛错', () => {
  const buf = new StreamBuf({ highWaterMark: 16, onData: () => {} });
  buf.end();
  assert.throws(() => buf.write(Buffer.from([1])), /write\(\) after end\(\)/);
});

test('end：同步消费者下先冲刷完再派发 finish', () => {
  const seen = [];
  const events = [];
  const buf = new StreamBuf({ highWaterMark: 16, onData: (c) => seen.push(c[0]) });
  buf.on('finish', () => events.push('finish'));
  buf.write(Buffer.from([1]));
  buf.write(Buffer.from([2]));
  buf.end();
  assert.deepEqual(seen, [1, 2]);
  assert.deepEqual(events, ['finish']);
});

test('end：缓冲为空时立即派发 finish', () => {
  const events = [];
  const buf = new StreamBuf({ highWaterMark: 16, onData: () => {} });
  buf.on('finish', () => events.push('finish'));
  buf.end();
  assert.deepEqual(events, ['finish']);
});

test('空 chunk：合法、被原样派发、不计入 bufferedLength', () => {
  const seen = [];
  const buf = new StreamBuf({ highWaterMark: 16, onData: (c) => seen.push(c) });
  const ok = buf.write(Buffer.alloc(0));
  assert.equal(ok, true);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].length, 0);
  assert.equal(buf.bufferedLength, 0);
});

test('消费者抛错：派发 error 事件，错误对象原样传出，write 本身不抛', () => {
  const boom = new Error('consumer failed');
  const seen = [];
  const errors = [];
  const buf = new StreamBuf({
    highWaterMark: 16,
    onData: (c) => {
      seen.push(c[0]);
      if (c[0] === 2) throw boom;
    },
  });
  buf.on('error', (e) => errors.push(e));

  buf.write(Buffer.from([1]));
  let thrown = null;
  try {
    buf.write(Buffer.from([2]));
  } catch (e) {
    thrown = e;
  }
  assert.equal(thrown, null);
  assert.deepEqual(seen, [1, 2]);
  assert.equal(errors.length, 1);
  assert.equal(errors[0], boom);
});

test('多个实例之间互不影响', () => {
  const seenA = [];
  const seenB = [];
  const a = new StreamBuf({ highWaterMark: 8, onData: (c) => seenA.push(c[0]) });
  const b = new StreamBuf({ highWaterMark: 8, onData: (c) => seenB.push(c[0]) });
  a.write(Buffer.from([1]));
  b.write(Buffer.from([2]));
  a.write(Buffer.from([3]));
  assert.deepEqual(seenA, [1, 3]);
  assert.deepEqual(seenB, [2]);
});
