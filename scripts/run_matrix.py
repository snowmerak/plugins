import subprocess
import time
import re
import sys
import os

languages = {
    "Go": {
        "args": ["./go/ringbuf-go-demo/ringbuf-go-demo"],
        "cwd": None,
        "env": None
    },
    "Rust": {
        "args": ["./rust/target/debug/ringbuf-rust"],
        "cwd": None,
        "env": None
    },
    "Python": {
        "args": ["/home/merak/.local/bin/uv", "run", "--no-sync", "python", "-m", "ringbuf_py_demo.main"],
        "cwd": "python/ringbuf_py",
        "env": lambda: dict(os.environ, UV_PROJECT_ENVIRONMENT="../.venv_wsl")
    },
    "TypeScript": {
        "args": ["node", "--import", "tsx", "src/main.ts"],
        "cwd": "typescript/ringbuf-ts-demo",
        "env": lambda: dict(os.environ, PATH="/home/linuxbrew/.linuxbrew/bin:" + os.environ.get("PATH", ""))
    }
}

COUNT = 100
SIZE = 1024
PATH = "/tmp/test_shm_bench"

matrix = {}

for host_name, host_cfg in languages.items():
    matrix[host_name] = {}
    for plugin_name, plugin_cfg in languages.items():
        print(f"[{host_name} -> {plugin_name}] Starting...", flush=True)
        
        # Cleanup files
        subprocess.run(f"rm -f {PATH}*", shell=True)
        
        # Build commands
        host_args = host_cfg["args"] + ["--role", "Host", "--path", PATH, "--count", str(COUNT), "--size", str(SIZE)]
        plugin_args = plugin_cfg["args"] + ["--role", "Plugin", "--path", PATH, "--count", str(COUNT), "--size", str(SIZE)]
        
        host_env = host_cfg["env"]() if host_cfg["env"] else None
        plugin_env = plugin_cfg["env"]() if plugin_cfg["env"] else None
        
        # Launch Host in background
        host_proc = subprocess.Popen(
            host_args,
            cwd=host_cfg["cwd"],
            env=host_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        time.sleep(0.5)
        
        # Launch Plugin in foreground with a 5-second timeout
        plugin_stdout = ""
        plugin_stderr = ""
        try:
            plugin_proc = subprocess.run(
                plugin_args,
                cwd=plugin_cfg["cwd"],
                env=plugin_env,
                capture_output=True,
                text=True,
                timeout=5
            )
            plugin_stdout = plugin_proc.stdout
            plugin_stderr = plugin_proc.stderr
        except subprocess.TimeoutExpired as e:
            plugin_stdout = e.stdout or ""
            plugin_stderr = e.stderr or ""
            print(f"  Plugin {plugin_name} timeout expired!", flush=True)
            # Kill leftover processes
            subprocess.run("pkill -f -9 ringbuf-go-demo; pkill -f -9 ringbuf-rust; pkill -f -9 python; pkill -f -9 node", shell=True)
        
        # Wait for Host to finish with a 5-second timeout
        host_stdout = ""
        host_stderr = ""
        try:
            stdout, stderr = host_proc.communicate(timeout=5)
            host_stdout = stdout
            host_stderr = stderr
        except subprocess.TimeoutExpired:
            host_proc.kill()
            stdout, stderr = host_proc.communicate()
            host_stdout = stdout
            host_stderr = stderr
            print(f"  Host {host_name} timeout expired!", flush=True)
            # Kill leftover processes
            subprocess.run("pkill -f -9 ringbuf-go-demo; pkill -f -9 ringbuf-rust; pkill -f -9 python; pkill -f -9 node", shell=True)
        
        # Parse Host output for BENCHMARK_RESULT
        match = re.search(r"BENCHMARK_RESULT:\s*(\d+)\s+rounds,\s*total time:\s*[\d\.\w\s\-]+,\s*([\d\.]+)\s+ops/sec,\s*avg latency:\s*([\d\.]+)\s+us", host_stdout)
        if match:
            ops = float(match.group(2))
            latency = float(match.group(3))
            matrix[host_name][plugin_name] = (ops, latency)
            print(f"  Result: {ops:.2f} ops/sec, {latency:.2f} us latency", flush=True)
        else:
            print(f"  Failed! Host stdout:\n{host_stdout}\nHost stderr:\n{host_stderr}\nPlugin stdout:\n{plugin_stdout}\nPlugin stderr:\n{plugin_stderr}", flush=True)
            matrix[host_name][plugin_name] = (0.0, 0.0)

# Print Matrix Table
print("\n### Benchmark Matrix (1KB messages, 100 rounds)\n", flush=True)
header = "| Host \\ Plugin | " + " | ".join(languages.keys()) + " |"
separator = "| --- | " + " | ".join(["---"] * len(languages)) + " |"
print(header, flush=True)
print(separator, flush=True)
for host_name in languages.keys():
    row = f"| {host_name} | "
    cols = []
    for plugin_name in languages.keys():
        ops, latency = matrix[host_name][plugin_name]
        if ops > 0:
            cols.append(f"{ops:.0f} ops/s<br>({latency:.1f} μs)")
        else:
            cols.append("FAIL")
    row += " | ".join(cols) + " |"
    print(row, flush=True)
