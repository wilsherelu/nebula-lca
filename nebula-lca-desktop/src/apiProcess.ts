import { ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
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

function checkHealth(baseUrl: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const req = http.get(`${baseUrl}/api/health`, { timeout: 3000 }, (resp) => {
      resp.resume();
      if (resp.statusCode && resp.statusCode >= 200 && resp.statusCode < 300) {
        resolve();
      } else {
        reject(new Error(`health status ${resp.statusCode ?? "unknown"}`));
      }
    });
    req.on("timeout", () => {
      req.destroy(new Error("health request timed out"));
    });
    req.on("error", reject);
  });
}

async function waitForHealth(baseUrl: string, logPath: string, timeoutMs = 90000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await checkHealth(baseUrl);
      return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 800));
  }
  const message = lastError instanceof Error ? lastError.message : "API health check timed out";
  fs.appendFileSync(logPath, `[api-health-failed] ${message}\n`);
  throw new Error(`API health check failed: ${message}`);
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
  fs.mkdirSync(path.dirname(paths.database), { recursive: true });
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
  const apiLogPath = path.join(paths.logs, "api.log");
  fs.appendFileSync(apiLogPath, `[api-start] exe=${paths.apiExe} cwd=${paths.userData} db=${paths.database} baseUrl=${baseUrl}\n`);
  attachLog(child, apiLogPath);
  child.on("error", (error) => {
    fs.appendFileSync(apiLogPath, `[api-error] ${String(error)}\n`);
  });
  await waitForHealth(baseUrl, apiLogPath);
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
