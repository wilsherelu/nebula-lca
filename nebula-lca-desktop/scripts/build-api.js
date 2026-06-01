import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "..");
const apiRoot = path.join(repoRoot, "nebula-lca-api");
const distDir = path.join(apiRoot, "dist", "nebula-lca-api");
const distExe = path.join(distDir, process.platform === "win32" ? "nebula-lca-api.exe" : "nebula-lca-api");
const targetDir = path.join(process.cwd(), "resources", "api");

execFileSync("python", ["-m", "PyInstaller", "pyinstaller/nebula-lca-api.spec", "--noconfirm"], {
  cwd: apiRoot,
  stdio: "inherit",
  shell: process.platform === "win32",
});
fs.rmSync(targetDir, { recursive: true, force: true });
fs.cpSync(distDir, targetDir, { recursive: true });
if (!fs.existsSync(path.join(targetDir, path.basename(distExe)))) {
  throw new Error(`API executable was not copied from ${distExe}`);
}
