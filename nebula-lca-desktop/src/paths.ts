import { app } from "electron";
import path from "node:path";

export type DesktopPaths = {
  userData: string;
  logs: string;
  runtime: string;
  importCache: string;
  database: string;
  credentialKeyFile: string;
  webRoot: string;
  apiExe: string;
};

export function resolveDesktopPaths(): DesktopPaths {
  const userData = app.getPath("userData");
  const appRoot = app.getAppPath();
  const devRoot = path.basename(appRoot) === "dist" ? path.dirname(appRoot) : appRoot;
  const resourcesRoot = app.isPackaged ? process.resourcesPath : path.resolve(devRoot, "resources");
  return {
    userData,
    logs: path.join(userData, "logs"),
    runtime: path.join(userData, "runtime"),
    importCache: path.join(userData, "import-cache"),
    database: path.join(userData, "nebula-lca.db"),
    credentialKeyFile: path.join(userData, "runtime", "secrets", "data_platform_credential.key"),
    webRoot: path.join(resourcesRoot, "web"),
    apiExe: path.join(resourcesRoot, "api", process.platform === "win32" ? "nebula-lca-api.exe" : "nebula-lca-api"),
  };
}
