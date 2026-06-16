import { newConnection, ROLE_HOST, ROLE_PLUGIN } from 'ringbuf-ts';

async function main() {
    let role = "Host";
    let basePath = "shm_comm_cross";
    let count = 1;
    let size = 1024;

    const args = process.argv;
    for (let i = 2; i < args.length; i++) {
        if (args[i] === '--role' && i + 1 < args.length) {
            role = args[i + 1];
            i++;
        } else if (args[i] === '--path' && i + 1 < args.length) {
            basePath = args[i + 1];
            i++;
        } else if (args[i] === '--count' && i + 1 < args.length) {
            count = parseInt(args[i + 1], 10) || 1;
            i++;
        } else if (args[i] === '--size' && i + 1 < args.length) {
            size = parseInt(args[i + 1], 10) || 1024;
            i++;
        }
    }

    const numSlots = count > 1 ? 8 : 4;
    const slotSize = count > 1 ? 4096 : 1024;

    if (count > 1) {
        console.log(`Starting TypeScript Ringbuffer Demo as ${role} (Benchmark: count=${count}, size=${size})...`);
    } else {
        console.log(`Starting TypeScript Ringbuffer Demo as ${role}...`);
    }

    let writer;
    let reader;
    try {
        [writer, reader] = await newConnection(
            basePath,
            role,
            numSlots,
            slotSize,
            15000, // 15 seconds
            15000  // 15 seconds
        );
    } catch (e) {
        console.error("failed to create connection:", e);
        process.exit(1);
    }

    try {
        if (count > 1) {
            const payload = Buffer.alloc(size, 'a');
            if (role === ROLE_HOST) {
                const start = Date.now();
                for (let i = 0; i < count; i++) {
                    await writer.write(payload);
                    await reader.readFull(size);
                }
                const elapsed = Date.now() - start; // in ms
                const ops = (count / (elapsed / 1000.0));
                const latency = (elapsed * 1000.0) / count; // in microseconds
                console.log(`BENCHMARK_RESULT: ${count} rounds, total time: ${elapsed / 1000.0}s, ${ops.toFixed(2)} ops/sec, avg latency: ${latency.toFixed(2)} us`);
            } else {
                for (let i = 0; i < count; i++) {
                    await reader.readFull(size);
                    await writer.write(payload);
                }
            }
        } else {
            if (role === ROLE_HOST) {
                const msg = Buffer.from("hello from ts host");
                console.log(`Host writing: '${msg.toString()}'`);
                await writer.write(msg);

                const buf = await reader.read(100);
                console.log(`Host read: '${buf.toString()}'`);
            } else {
                const buf = await reader.read(100);
                console.log(`Plugin read: '${buf.toString()}'`);

                const msg = Buffer.from("hello from ts plugin");
                console.log(`Plugin writing: '${msg.toString()}'`);
                await writer.write(msg);
            }
        }
        console.log("TypeScript Demo finished successfully.");
    } catch (e) {
        console.error("execution failed:", e);
        process.exit(1);
    } finally {
        await new Promise(r => setTimeout(r, 100));
        writer.close();
        reader.close();
    }
}

main();
