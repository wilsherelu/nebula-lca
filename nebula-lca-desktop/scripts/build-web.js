import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "..");
const webRoot = path.join(repoRoot, "nebula-lca-web");
const target = path.join(process.cwd(), "resources", "web");

execFileSync("npm", ["run", "build"], { cwd: webRoot, stdio: "inherit", shell: process.platform === "win32" });
fs.rmSync(target, { recursive: true, force: true });
fs.cpSync(path.join(webRoot, "dist"), target, { recursive: true });
