import { test } from 'node:test';
import * as assert from 'node:assert';
import * as path from 'path';
import * as os from 'os';
import * as fs from 'fs';
import * as net from 'net';
import {
    RingBuffer,
    Writer,
    Reader,
    newConnection,
    ROLE_HOST,
    ROLE_PLUGIN,
    TimeoutError,
    SocketSignalManager
} from '../src/index.js';

async function localSocketPair(): Promise<[net.Socket, net.Socket]> {
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as net.AddressInfo).port;

    const acceptPromise = new Promise<net.Socket>((resolve) => {
        server.on('connection', (socket) => {
            server.close();
            resolve(socket);
        });
    });

    const client = net.connect(port, '127.0.0.1');
    const serverSocket = await acceptPromise;

    return [serverSocket, client];
}

function getTempFilePath(prefix: string): string {
    return path.join(os.tmpdir(), `${prefix}_${Math.random().toString(36).substring(2, 9)}`);
}

test('RingBuffer - Basic', async () => {
    const filePath = getTempFilePath('shm_basic');
    const rb = new RingBuffer(filePath, 2, 64);
    rb.clear();

    const [s1, s2] = await localSocketPair();
    const sigMgr1 = new SocketSignalManager(s1);
    const sigMgr2 = new SocketSignalManager(s2);

    try {
        const payload1 = Buffer.from('hello world');
        await rb.write(payload1, 42n, 10, sigMgr1);

        const [readPayload, seq] = await rb.read(10, sigMgr2);
        assert.strictEqual(seq, 42n);
        assert.deepStrictEqual(readPayload, payload1);
    } finally {
        sigMgr1.close();
        sigMgr2.close();
        rb.close();
        try { fs.unlinkSync(filePath); } catch (e) {}
    }
});

test('RingBuffer - Timeout and Drop', async () => {
    const filePath = getTempFilePath('shm_timeout');
    const rb = new RingBuffer(filePath, 2, 64);
    rb.clear();

    const [s1, s2] = await localSocketPair();
    const sigMgr1 = new SocketSignalManager(s1);
    const sigMgr2 = new SocketSignalManager(s2);

    try {
        await rb.write(Buffer.from('msg1'), 1n, 10, sigMgr1);
        await rb.write(Buffer.from('msg2'), 2n, 10, sigMgr1);

        // Buffer full, write expects timeout
        await assert.rejects(
            async () => {
                await rb.write(Buffer.from('msg3'), 3n, 20, sigMgr1);
            },
            (err) => err instanceof TimeoutError
        );

        // writeIndex should advance to 1
        assert.strictEqual(rb.writeIndex, 1);
    } finally {
        sigMgr1.close();
        sigMgr2.close();
        rb.close();
        try { fs.unlinkSync(filePath); } catch (e) {}
    }
});

test('Connection - Basic', { skip: process.platform === 'win32' }, async () => {
    const baseDir = getTempFilePath('shm_conn');
    fs.mkdirSync(baseDir, { recursive: true });
    const basePath = path.join(baseDir, 'shm_comm');

    let hostWriter: Writer | null = null;
    let hostReader: Reader | null = null;
    let pluginWriter: Writer | null = null;
    let pluginReader: Reader | null = null;

    try {
        const hostPromise = newConnection(basePath, ROLE_HOST, 2, 64, 500, 500);
        // Wait a tiny bit for server listener to bind
        await new Promise(r => setTimeout(r, 20));
        const pluginPromise = newConnection(basePath, ROLE_PLUGIN, 2, 64, 500, 500);

        [hostWriter, hostReader] = await hostPromise;
        [pluginWriter, pluginReader] = await pluginPromise;

        // Host writes, Plugin reads
        const msgFromHost = Buffer.from('hello from host');
        await hostWriter.write(msgFromHost);

        const readBufPlugin = await pluginReader.read(100);
        assert.deepStrictEqual(readBufPlugin, msgFromHost);

        // Plugin writes, Host reads
        const msgFromPlugin = Buffer.from('hello from plugin');
        await pluginWriter.write(msgFromPlugin);

        const readBufHost = await hostReader.read(100);
        assert.deepStrictEqual(readBufHost, msgFromPlugin);

    } finally {
        if (hostWriter) hostWriter.close();
        if (hostReader) hostReader.close();
        if (pluginWriter) pluginWriter.close();
        if (pluginReader) pluginReader.close();

        // Cleanup
        try {
            fs.rmSync(baseDir, { recursive: true, force: true });
        } catch (e) {}
    }
});

test('RingBuffer - Large Data Integrity', async () => {
    const filePath = getTempFilePath('shm_large');
    const rb = new RingBuffer(filePath, 2, 64);
    rb.clear();

    const [s1, s2] = await localSocketPair();
    const sigMgr1 = new SocketSignalManager(s1);
    const sigMgr2 = new SocketSignalManager(s2);

    const writer = new Writer(rb, sigMgr1, 1000);
    const reader = new Reader(rb, sigMgr2, 1000);

    const largePayload = Buffer.from(Array.from({ length: 250 }, (_, i) => i % 256));

    const readResult: Buffer[] = [];
    let readErr: any = null;

    const readPromise = (async () => {
        try {
            const res = await reader.readFull(250);
            readResult.push(res);
        } catch (e) {
            readErr = e;
        }
    })();

    try {
        const written = await writer.write(largePayload);
        assert.strictEqual(written, 250);
        await readPromise;
    } finally {
        writer.close();
        reader.close();
        try { fs.unlinkSync(filePath); } catch (e) {}
    }

    if (readErr) throw readErr;
    assert.deepStrictEqual(readResult[0], largePayload);
});
