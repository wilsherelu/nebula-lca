import { app, BrowserWindow, dialog, ipcMain, shell } from "electron";
import fs from "node:fs";
import path from "node:path";
import { startApiProcess, ApiProcessHandle } from "./apiProcess.js";
import { resolveDesktopPaths } from "./paths.js";

app.setName("nebula-lca-desktop");

let apiHandle: ApiProcessHandle | null = null;
const smokeMode = process.argv.includes("--smoke");
let quitting = false;
const rendererLogPath = path.join(resolveDesktopPaths().logs, "renderer.log");

function writeRendererLog(...args: unknown[]): void {
  const line = `[${new Date().toISOString()}] ${args.map((arg) => String(arg)).join(" ")}\n`;
  try {
    fs.mkdirSync(path.dirname(rendererLogPath), { recursive: true });
    fs.appendFileSync(rendererLogPath, line);
  } catch {
    // Ignore diagnostic logging failures.
  }
}

function loadingHtml(message: string): string {
  return `data:text/html;charset=utf-8,${encodeURIComponent(
    `<html><body style="font-family:Segoe UI,sans-serif;margin:32px;color:#203743"><h2>Nebula LCA</h2><p>${message}</p></body></html>`,
  )}`;
}

async function createWindow(): Promise<void> {
  const paths = resolveDesktopPaths();
  const appRoot = app.getAppPath();
  const preloadPath =
    path.basename(appRoot) === "dist" ? path.join(appRoot, "preload.cjs") : path.join(appRoot, "dist", "preload.cjs");
  const win = new BrowserWindow({
    width: 1440,
    height: 920,
    minWidth: 1100,
    minHeight: 720,
    show: false,
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  win.loadURL(loadingHtml("Starting local API..."));
  win.once("ready-to-show", () => win.show());

  win.webContents.on("console-message", (_event, _level, message) => {
    writeRendererLog("RENDERER-CONSOLE:", message);
  });
  win.webContents.on("did-fail-load", (_event, errorCode, errorDescription, validatedURL) => {
    writeRendererLog("DID-FAIL-LOAD:", errorCode, errorDescription, validatedURL);
  });
  win.webContents.on("render-process-gone", (_event, details) => {
    writeRendererLog("RENDER-PROCESS-GONE:", JSON.stringify(details));
  });

  apiHandle = await startApiProcess(paths);
  const apiBase = `${apiHandle.baseUrl}/api`;
  win.webContents.on("did-finish-load", () => {
    win.webContents.send("set-api-base", apiBase);
  });
  process.env.NEBULA_API_BASE = apiBase;

  const indexPath = path.join(paths.webRoot, "index.html");
  if (!fs.existsSync(indexPath)) {
    await win.loadURL(loadingHtml(`Web resources not found: ${indexPath}`));
    return;
  }
  await win.loadFile(indexPath);
  if (smokeMode) {
    setTimeout(async () => {
      const handle = apiHandle;
      apiHandle = null;
      if (handle) await handle.stop();
      quitting = true;
      app.exit(0);
    }, 1500).unref();
  }
}

app.whenReady().then(async () => {
  const paths = resolveDesktopPaths();
  ipcMain.handle("desktop:openLogs", () => shell.openPath(paths.logs));
  ipcMain.handle("desktop:getDiagnostics", () => ({
    appVersion: app.getVersion(),
    apiBase: apiHandle?.baseUrl,
    userData: paths.userData,
    runtime: paths.runtime,
    importCache: paths.importCache,
    logs: paths.logs,
  }));
  ipcMain.handle("desktop:chooseImportFile", async () => {
    const result = await dialog.showOpenDialog({
      properties: ["openFile"],
      filters: [
        { name: "Import packages", extensions: ["7z", "zip", "json", "xlsx", "csv"] },
        { name: "All files", extensions: ["*"] },
      ],
    });
    return result.canceled ? null : result.filePaths[0] ?? null;
  });

  try {
    await createWindow();
  } catch (error) {
    dialog.showErrorBox("Nebula LCA startup failed", error instanceof Error ? error.message : String(error));
    app.quit();
  }
});

app.on("window-all-closed", () => {
  app.quit();
});

app.on("before-quit", async (event) => {
  if (quitting || !apiHandle) return;
  event.preventDefault();
  const handle = apiHandle;
  apiHandle = null;
  await handle.stop();
  quitting = true;
  app.quit();
});
