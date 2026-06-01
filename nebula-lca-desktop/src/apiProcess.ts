import { ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import net from "node:net";
import { DesktopPaths } from "./paths.js";

export type ApiProcessHandle = {
  port: number;
  baseUrl: string;
  stop: () => Promise<void>;
};

function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => {
        if (address && typeof address === "object") resolve(address.port);
        else reject(new Error("Failed to allocate localhost port"));
      });
    });
  });
}

async function waitForHealth(baseUrl: string, timeoutMs = 90000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(`${baseUrl}/api/health`, { cache: "no-store" });
      if (resp.ok) return;
      lastError = new Error(`health status ${resp.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 800));
  }
  throw lastError instanceof Error ? lastError : new Error("API health check timed out");
}

function attachLog(child: ChildProcessWithoutNullStreams, logPath: string): void {
  const stream = fs.createWriteStream(logPath, { flags: "a" });
  child.stdout.pipe(stream, { end: false });
  child.stderr.pipe(stream, { end: false });
  child.on("exit", (code, signal) => {
    stream.write(`\n[api-exit] code=${code ?? ""} signal=${signal ?? ""}\n`);
    stream.end();
  });
}

export async function startApiProcess(paths: DesktopPaths): Promise<ApiProcessHandle> {
  fs.mkdirSync(paths.logs, { recursive: true });
  fs.mkdirSync(paths.runtime, { recursive: true });
  fs.mkdirSync(paths.importCache, { recursive: true });
  fs.mkdirSync(path.dirname(paths.credentialKeyFile), { recursive: true });

  const port = await findFreePort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const env = {
    ...process.env,
    NEBULA_DESKTOP: "1",
    NEBULA_DATA_DIR: paths.userData,
    NEBULA_DB_PATH: paths.database,
    DATABASE_URL: `sqlite:///${paths.database.replace(/\\/g, "/")}`,
    NEBULA_LCA_RUNTIME_ROOT: paths.runtime,
    NEBULA_IMPORT_CACHE_DIR: paths.importCache,
    DATA_PLATFORM_CREDENTIAL_KEY_FILE: paths.credentialKeyFile,
    NEBULA_API_PORT: String(port),
    NEBULA_CORS_ORIGINS: `${baseUrl},app://nebula,file://`,
  };
  const child = spawn(paths.apiExe, [], {
    env,
    cwd: paths.userData,
    windowsHide: true,
  });
  attachLog(child, path.join(paths.logs, "api.log"));
  child.on("error", (error) => {
    fs.appendFileSync(path.join(paths.logs, "api.log"), `[api-error] ${String(error)}\n`);
  });
  await waitForHealth(baseUrl);
  return {
    port,
    baseUrl,
    stop: () => new Promise((resolve) => {
      if (child.killed || child.exitCode !== null) {
        resolve();
        return;
      }
      child.once("exit", () => resolve());
      child.kill();
      setTimeout(() => {
        if (child.exitCode === null) child.kill("SIGKILL");
      }, 3000).unref();
    }),
  };
}
