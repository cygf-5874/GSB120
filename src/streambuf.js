/**
 * streambuf —— 带背压的字节缓冲。
 *
 * 把上游写进来的 chunk 依次交给 `onData`；消费者每消费一块可以返回一个 Promise，
 * 缓冲会在它落定之后才继续。
 *
 * 对外保证（8 条）见仓库根目录的 README.md。
 */
import { EventEmitter } from 'node:events';

/** 默认高水位（字节）。 */
export const DEFAULT_HIGH_WATER_MARK = 16;

/**
 * 把入参归一成 Buffer。
 * @param {Buffer|Uint8Array|string} chunk
 * @returns {Buffer}
 */
function toChunk(chunk) {
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === 'string') return Buffer.from(chunk, 'utf8');
  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  throw new TypeError('chunk must be a Buffer, Uint8Array or string');
}

const isThenable = (value) =>
  value !== null && typeof value === 'object' && typeof value.then === 'function';

export class StreamBuf extends EventEmitter {
  /**
   * @param {{highWaterMark?: number, onData: (chunk: Buffer) => unknown}} options
   */
  constructor(options = {}) {
    super();
    const highWaterMark =
      options.highWaterMark === undefined ? DEFAULT_HIGH_WATER_MARK : options.highWaterMark;
    if (typeof highWaterMark !== 'number' || !Number.isFinite(highWaterMark) || highWaterMark < 1) {
      throw new TypeError('highWaterMark must be a positive finite number');
    }
    if (typeof options.onData !== 'function') {
      throw new TypeError('onData must be a function');
    }

    this._highWaterMark = highWaterMark;
    this._onData = options.onData;
    this._queue = [];
    // 已写入但消费者还没消费完的字节数（含已交给 onData、仍在等待其 Promise 的块）。
    this._buffered = 0;
    this._ended = false;
    this._finished = false;
    this._errored = false;
    this._backpressured = false;
    // 有一块已交给消费者、正在等待它的 Promise 落定。
    this._awaiting = false;
  }

  /** 高水位（字节）。 */
  get highWaterMark() {
    return this._highWaterMark;
  }

  /** 当前还压在缓冲里的字节数。 */
  get bufferedLength() {
    return this._buffered;
  }

  /**
   * 写入一个 chunk。
   * @param {Buffer|Uint8Array|string} chunk
   * @returns {boolean} `false` 表示本次写入到达了高水位，调用方应暂停
   */
  write(chunk) {
    if (this._ended) {
      throw new Error('write() after end()');
    }
    if (this._errored) {
      return false;
    }

    const buf = toChunk(chunk);
    this._queue.push(buf);
    this._buffered += buf.length;

    // 判据取“把 chunk 放进缓冲之后、冲刷之前”这一刻（在途块仍计入），
    // 与同步消费者是否立刻消费完无关。
    const reachedHighWaterMark = this._buffered >= this._highWaterMark;
    if (reachedHighWaterMark) {
      this._backpressured = true;
    }

    this._flush();
    return !reachedHighWaterMark;
  }

  /** 声明写完；缓冲里的数据全部交给 `onData` 之后再派发 `finish`。 */
  end() {
    if (this._ended) return;
    this._ended = true;
    this._flush();
  }

  _finish() {
    if (this._finished || this._errored) return;
    this._finished = true;
    this.emit('finish');
  }

  _fail(error) {
    if (this._errored) return;
    this._errored = true;
    this.emit('error', error);
  }

  /**
   * 背压态下，水位首次降到高水位以下时补一次 drain（恰一次）。
   * 此刻未消费完的块（含在途块）仍计入 _buffered。
   */
  _maybeDrain() {
    if (this._backpressured && this._buffered < this._highWaterMark) {
      this._backpressured = false;
      this.emit('drain');
    }
  }

  /**
   * 按序把队首交给消费者。一块被完整消费（同步返回 / Promise 落定）之后
   * 才出队、才从 _buffered 扣除；抛错或拒绝时这块原样留在队首并停止冲刷。
   */
  _flush() {
    if (this._awaiting || this._errored) return;

    while (this._queue.length > 0) {
      const chunk = this._queue[0];

      let result;
      try {
        result = this._onData(chunk);
      } catch (error) {
        // 消费失败：这块没被消费，必须仍留在缓冲里，且不再派发后续 data。
        this._fail(error);
        return;
      }

      if (isThenable(result)) {
        this._awaiting = true;
        Promise.resolve(result).then(
          () => {
            this._awaiting = false;
            this._queue.shift();
            this._buffered -= chunk.length;
            this._maybeDrain();
            this._flush();
          },
          (error) => {
            this._awaiting = false;
            // 拒绝：这块仍留在队首，不丢弃。
            this._fail(error);
          },
        );
        return;
      }

      this._queue.shift();
      this._buffered -= chunk.length;
      this._maybeDrain();
    }

    if (this._ended) {
      this._finish();
    }
  }
}

export default StreamBuf;
