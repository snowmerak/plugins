import { newConnection, ROLE_HOST, ROLE_PLUGIN } from 'ringbuf-ts';

async function main() {
    let role = "Host";
    let basePath = "shm_comm_cross";

    const args = process.argv;
    for (let i = 2; i < args.length; i++) {
        if (args[i] === '--role' && i + 1 < args.length) {
            role = args[i + 1];
            i++;
        } else if (args[i] === '--path' && i + 1 < args.length) {
            basePath = args[i + 1];
            i++;
        }
    }

    console.log(`Starting TypeScript Ringbuffer Demo as ${role}...`);

    let writer;
    let reader;
    try {
        [writer, reader] = await newConnection(
            basePath,
            role,
            4,
            1024,
            15000, // 15 seconds
            15000  // 15 seconds
        );
    } catch (e) {
        console.error("failed to create connection:", e);
        process.exit(1);
    }

    try {
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
