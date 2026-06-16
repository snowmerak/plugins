import * as fs from 'fs';
import * as net from 'net';

export const STATE_EMPTY = 0;
export const STATE_WRITTEN = 1;

export const ROLE_HOST = "Host";
export const ROLE_PLUGIN = "Plugin";

export class TimeoutError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "TimeoutError";
    }
}

export class RingBuffer {
    public fd: number;
    public numSlots: number;
    public slotDataSize: number;
    public slotSize: number;
    public totalSize: number;
    public writeIndex = 0;
    public readIndex = 0;

    constructor(filePath: string, numSlots: number, slotDataSize: number) {
        if (numSlots <= 0 || slotDataSize <= 0) {
            throw new Error("invalid dimensions");
        }
        this.numSlots = numSlots;
        this.slotDataSize = slotDataSize;
        this.slotSize = 24 + slotDataSize;
        this.totalSize = numSlots * this.slotSize;

        try {
            this.fd = fs.openSync(filePath, 'r+');
        } catch (e) {
            this.fd = fs.openSync(filePath, 'w+');
        }
        fs.ftruncateSync(this.fd, this.totalSize);
    }

    close(): void {
        if (this.fd !== -1) {
            try {
                fs.closeSync(this.fd);
            } catch (e) {}
            this.fd = -1;
        }
    }

    clear(): void {
        for (let i = 0; i < this.numSlots; i++) {
            this.setFlag(i, STATE_EMPTY);
            this.setLen(i, 0);
            this.setSeq(i, 0n);
            this.setWriterWaiting(i, 0);
            this.setReaderWaiting(i, 0);
        }
        this.writeIndex = 0;
        this.readIndex = 0;
    }

    private _offset(slot: number): number {
        return slot * this.slotSize;
    }

    getFlag(slot: number): number {
        const buf = Buffer.alloc(4);
        fs.readSync(this.fd, buf, 0, 4, this._offset(slot));
        return buf.readUInt32LE(0);
    }

    setFlag(slot: number, val: number): void {
        const buf = Buffer.alloc(4);
        buf.writeUInt32LE(val, 0);
        fs.writeSync(this.fd, buf, 0, 4, this._offset(slot));
    }

    getLen(slot: number): number {
        const buf = Buffer.alloc(4);
        fs.readSync(this.fd, buf, 0, 4, this._offset(slot) + 4);
        return buf.readUInt32LE(0);
    }

    setLen(slot: number, val: number): void {
        const buf = Buffer.alloc(4);
        buf.writeUInt32LE(val, 0);
        fs.writeSync(this.fd, buf, 0, 4, this._offset(slot) + 4);
    }

    getSeq(slot: number): bigint {
        const buf = Buffer.alloc(8);
        fs.readSync(this.fd, buf, 0, 8, this._offset(slot) + 8);
        return buf.readBigUInt64LE(0);
    }

    setSeq(slot: number, val: bigint | number): void {
        const buf = Buffer.alloc(8);
        buf.writeBigUInt64LE(BigInt(val), 0);
        fs.writeSync(this.fd, buf, 0, 8, this._offset(slot) + 8);
    }

    getWriterWaiting(slot: number): number {
        const buf = Buffer.alloc(4);
        fs.readSync(this.fd, buf, 0, 4, this._offset(slot) + 16);
        return buf.readUInt32LE(0);
    }

    setWriterWaiting(slot: number, val: number): void {
        const buf = Buffer.alloc(4);
        buf.writeUInt32LE(val, 0);
        fs.writeSync(this.fd, buf, 0, 4, this._offset(slot) + 16);
    }

    getReaderWaiting(slot: number): number {
        const buf = Buffer.alloc(4);
        fs.readSync(this.fd, buf, 0, 4, this._offset(slot) + 20);
        return buf.readUInt32LE(0);
    }

    setReaderWaiting(slot: number, val: number): void {
        const buf = Buffer.alloc(4);
        buf.writeUInt32LE(val, 0);
        fs.writeSync(this.fd, buf, 0, 4, this._offset(slot) + 20);
    }

    getPayload(slot: number, len: number): Buffer {
        const buf = Buffer.alloc(len);
        fs.readSync(this.fd, buf, 0, len, this._offset(slot) + 24);
        return buf;
    }

    setPayload(slot: number, payload: Buffer): void {
        fs.writeSync(this.fd, payload, 0, payload.length, this._offset(slot) + 24);
    }

    async write(payload: Buffer, seq: bigint | number, timeoutMs: number, sigMgr: SocketSignalManager): Promise<void> {
        if (payload.length > this.slotDataSize) {
            throw new Error(`payload size ${payload.length} exceeds slot capacity ${this.slotDataSize}`);
        }

        const start = Date.now();
        while (this.getFlag(this.writeIndex) !== STATE_EMPTY) {
            this.setWriterWaiting(this.writeIndex, 1);

            // Yield to execute a memory barrier
            await new Promise(r => setTimeout(r, 0));

            if (this.getFlag(this.writeIndex) === STATE_EMPTY) {
                this.setWriterWaiting(this.writeIndex, 0);
                break;
            }

            const remaining = timeoutMs > 0 ? timeoutMs - (Date.now() - start) : 0;
            if (timeoutMs > 0 && remaining <= 0) {
                this.setWriterWaiting(this.writeIndex, 0);
                const idx = this.writeIndex;
                this.writeIndex = (this.writeIndex + 1) % this.numSlots;
                throw new TimeoutError(`write timeout on slot ${idx} waiting for empty signal`);
            }

            try {
                await sigMgr.waitWrite(remaining);
            } catch (err) {
                this.setWriterWaiting(this.writeIndex, 0);
                const idx = this.writeIndex;
                this.writeIndex = (this.writeIndex + 1) % this.numSlots;
                throw err;
            }

            this.setWriterWaiting(this.writeIndex, 0);
        }

        const slotIdx = this.writeIndex;
        this.setPayload(slotIdx, payload);
        this.setLen(slotIdx, payload.length);
        this.setSeq(slotIdx, seq);

        this.setFlag(slotIdx, STATE_WRITTEN);

        if (this.getReaderWaiting(slotIdx) === 1) {
            this.setReaderWaiting(slotIdx, 0);
            try {
                sigMgr.send(0x01);
            } catch (e) {}
        }

        this.writeIndex = (this.writeIndex + 1) % this.numSlots;
    }

    async read(timeoutMs: number, sigMgr: SocketSignalManager): Promise<[Buffer, bigint]> {
        const start = Date.now();
        while (this.getFlag(this.readIndex) !== STATE_WRITTEN) {
            this.setReaderWaiting(this.readIndex, 1);

            // Yield to execute a memory barrier
            await new Promise(r => setTimeout(r, 0));

            if (this.getFlag(this.readIndex) === STATE_WRITTEN) {
                this.setReaderWaiting(this.readIndex, 0);
                break;
            }

            const remaining = timeoutMs > 0 ? timeoutMs - (Date.now() - start) : 0;
            if (timeoutMs > 0 && remaining <= 0) {
                this.setReaderWaiting(this.readIndex, 0);
                throw new TimeoutError(`read timeout on slot ${this.readIndex} waiting for write signal`);
            }

            try {
                await sigMgr.waitRead(remaining);
            } catch (err) {
                this.setReaderWaiting(this.readIndex, 0);
                throw err;
            }

            this.setReaderWaiting(this.readIndex, 0);
        }

        const slotIdx = this.readIndex;
        const len = this.getLen(slotIdx);
        const seq = this.getSeq(slotIdx);

        if (len > this.slotDataSize) {
            throw new Error(`invalid payload length ${len} on slot ${slotIdx}`);
        }

        const payload = this.getPayload(slotIdx, len);

        this.setFlag(slotIdx, STATE_EMPTY);

        if (this.getWriterWaiting(slotIdx) === 1) {
            this.setWriterWaiting(slotIdx, 0);
            try {
                sigMgr.send(0x02);
            } catch (e) {}
        }

        this.readIndex = (this.readIndex + 1) % this.numSlots;
        return [payload, seq];
    }
}

export class SocketSignalManager {
    private socket: net.Socket;
    private writeWaiters: (() => void)[] = [];
    private readWaiters: (() => void)[] = [];

    constructor(socket: net.Socket) {
        this.socket = socket;
        this.socket.on('error', () => {}); // Prevent unhandled 'error' crash on ECONNRESET
        this.socket.on('data', (chunk) => {
            for (const byte of chunk) {
                if (byte === 0x01) {
                    const cb = this.readWaiters.shift();
                    if (cb) cb();
                } else if (byte === 0x02) {
                    const cb = this.writeWaiters.shift();
                    if (cb) cb();
                }
            }
        });
    }

    waitWrite(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            let timer: NodeJS.Timeout | null = null;
            const cb = () => {
                if (timer) clearTimeout(timer);
                resolve();
            };
            this.writeWaiters.push(cb);

            if (timeoutMs > 0) {
                timer = setTimeout(() => {
                    const idx = this.writeWaiters.indexOf(cb);
                    if (idx !== -1) {
                        this.writeWaiters.splice(idx, 1);
                    }
                    reject(new TimeoutError("write timeout"));
                }, timeoutMs);
            }
        });
    }

    waitRead(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            let timer: NodeJS.Timeout | null = null;
            const cb = () => {
                if (timer) clearTimeout(timer);
                resolve();
            };
            this.readWaiters.push(cb);

            if (timeoutMs > 0) {
                timer = setTimeout(() => {
                    const idx = this.readWaiters.indexOf(cb);
                    if (idx !== -1) {
                        this.readWaiters.splice(idx, 1);
                    }
                    reject(new TimeoutError("read timeout"));
                }, timeoutMs);
            }
        });
    }

    send(token: number): void {
        this.socket.write(Buffer.from([token]));
    }

    close(): void {
        this.socket.end();
    }
}

class Mutex {
    private queue: (() => void)[] = [];
    private locked = false;

    acquire(): Promise<() => void> {
        return new Promise((resolve) => {
            const release = () => {
                if (this.queue.length > 0) {
                    const next = this.queue.shift();
                    if (next) next();
                } else {
                    this.locked = false;
                }
            };

            if (this.locked) {
                this.queue.push(() => resolve(release));
            } else {
                this.locked = true;
                resolve(release);
            }
        });
    }
}

export class Writer {
    private rb: RingBuffer;
    private sigMgr: SocketSignalManager;
    private writeTimeoutMs: number;
    private nextWriteSeq = 0n;
    private writeMutex = new Mutex();

    constructor(rb: RingBuffer, sigMgr: SocketSignalManager, writeTimeoutMs: number) {
        this.rb = rb;
        this.sigMgr = sigMgr;
        this.writeTimeoutMs = writeTimeoutMs;
    }

    async write(p: Buffer): Promise<number> {
        const release = await this.writeMutex.acquire();
        try {
            const total = p.length;
            let offset = 0;
            while (offset < total) {
                const chunkSize = Math.min(total - offset, this.rb.slotDataSize);
                const chunk = p.subarray(offset, offset + chunkSize);
                await this.rb.write(chunk, this.nextWriteSeq, this.writeTimeoutMs, this.sigMgr);
                this.nextWriteSeq++;
                offset += chunkSize;
            }
            return total;
        } finally {
            release();
        }
    }

    close(): void {
        this.sigMgr.close();
        this.rb.close();
    }
}

export class Reader {
    private rb: RingBuffer;
    private sigMgr: SocketSignalManager;
    private readTimeoutMs: number;
    private readBuf: Buffer = Buffer.alloc(0);
    private readMutex = new Mutex();

    constructor(rb: RingBuffer, sigMgr: SocketSignalManager, readTimeoutMs: number) {
        this.rb = rb;
        this.sigMgr = sigMgr;
        this.readTimeoutMs = readTimeoutMs;
    }

    async read(n: number): Promise<Buffer> {
        const release = await this.readMutex.acquire();
        try {
            if (n <= 0) return Buffer.alloc(0);
            if (this.readBuf.length === 0) {
                const [payload] = await this.rb.read(this.readTimeoutMs, this.sigMgr);
                this.readBuf = payload;
            }

            const chunkSize = Math.min(n, this.readBuf.length);
            const res = this.readBuf.subarray(0, chunkSize);
            this.readBuf = this.readBuf.subarray(chunkSize);
            return Buffer.from(res);
        } finally {
            release();
        }
    }

    async readFull(n: number): Promise<Buffer> {
        const chunks: Buffer[] = [];
        let bytesRead = 0;
        while (bytesRead < n) {
            const chunk = await this.read(n - bytesRead);
            if (chunk.length === 0) {
                throw new Error("connection closed during readFull");
            }
            chunks.push(chunk);
            bytesRead += chunk.length;
        }
        return Buffer.concat(chunks);
    }

    close(): void {
        this.sigMgr.close();
        this.rb.close();
    }
}

export async function newConnection(
    basePath: string,
    role: string,
    numSlots: number,
    slotDataSize: number,
    writeTimeoutMs: number,
    readTimeoutMs: number
): Promise<[Writer, Reader]> {
    const sigSockPath = basePath + "_sig.sock";

    const writePath = role === ROLE_HOST ? basePath + "_writer" : basePath + "_reader";
    const readPath = role === ROLE_HOST ? basePath + "_reader" : basePath + "_writer";

    // Instantiate and clear shared memory ring buffers before connection accepted to prevent race conditions
    const writeRb = new RingBuffer(writePath, numSlots, slotDataSize);
    writeRb.clear();
    const readRb = new RingBuffer(readPath, numSlots, slotDataSize);

    let socket1: net.Socket;
    let socket2: net.Socket;

    if (role === ROLE_HOST) {
        if (fs.existsSync(sigSockPath)) {
            try {
                fs.unlinkSync(sigSockPath);
            } catch (e) {}
        }

        const server = net.createServer();
        server.listen(sigSockPath);

        const socketsPromise = new Promise<[net.Socket, net.Socket]>((resolve, reject) => {
            const accepted: net.Socket[] = [];
            server.on('connection', (socket) => {
                accepted.push(socket);
                if (accepted.length === 2) {
                    server.close();
                    resolve([accepted[0], accepted[1]]);
                }
            });
            server.on('error', (err) => reject(err));
        });

        [socket1, socket2] = await socketsPromise;
    } else if (role === ROLE_PLUGIN) {
        const connect = async () => {
            for (let i = 0; i < 100; i++) {
                try {
                    const socket = net.createConnection(sigSockPath);
                    await new Promise<void>((resolve, reject) => {
                        socket.on('connect', resolve);
                        socket.on('error', reject);
                    });
                    return socket;
                } catch (e) {
                    await new Promise(r => setTimeout(r, 10));
                }
            }
            throw new Error("plugin failed to dial sig connection");
        };

        socket1 = await connect();
        socket2 = await connect();
    } else {
        throw new Error(`invalid role: ${role}`);
    }

    const sigMgr1 = new SocketSignalManager(socket1);
    const sigMgr2 = new SocketSignalManager(socket2);

    let writer: Writer;
    let reader: Reader;

    if (role === ROLE_HOST) {
        writer = new Writer(writeRb, sigMgr1, writeTimeoutMs);
        reader = new Reader(readRb, sigMgr2, readTimeoutMs);
    } else {
        writer = new Writer(writeRb, sigMgr2, writeTimeoutMs);
        reader = new Reader(readRb, sigMgr1, readTimeoutMs);
    }

    return [writer, reader];
}
