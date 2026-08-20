import fs from "node:fs";
import path from "node:path";

const resourcesRoot = path.resolve(process.argv[2] || path.join(process.cwd(), "resources"));
const internalRoot = path.join(resourcesRoot, "api", "_internal");
const dataRoot = path.join(internalRoot, "data");
const mappingRoot = path.join(dataRoot, "flow_mappings");
const runtimeRoot = path.join(internalRoot, "runtime", "ef31");

function fail(message) {
  throw new Error(`Release asset audit failed: ${message}`);
}

function walk(root) {
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(root, entry.name);
    return entry.isDirectory() ? walk(target) : [target];
  });
}

const firstPartyFiles = [...walk(dataRoot), ...walk(path.join(internalRoot, "runtime"))];
const forbidden = firstPartyFiles.filter((file) => {
  const relative = path.relative(internalRoot, file).replaceAll("\\", "/").toLowerCase();
  const extension = path.extname(file).toLowerCase();
  return extension === ".docx"
    || extension === ".7z"
    || relative.includes("ghg_co2")
    || relative.includes("smoke")
    || relative.includes("data_platform_credential")
    || relative.endsWith("lca_demo.db");
});
if (forbidden.length) fail(`forbidden files: ${forbidden.map((file) => path.relative(internalRoot, file)).join(", ")}`);

const publicMappingDirs = fs.readdirSync(mappingRoot, { withFileTypes: true })
  .filter((entry) => entry.isDirectory() && entry.name.startsWith("nebula-flow-mapping-"));
if (publicMappingDirs.length !== 1) fail(`expected one public mapping release, found ${publicMappingDirs.length}`);
const publicMappingDir = path.join(mappingRoot, publicMappingDirs[0].name);
const manifest = JSON.parse(fs.readFileSync(path.join(publicMappingDir, "MANIFEST.json"), "utf8"));
const acceptance = JSON.parse(fs.readFileSync(path.join(publicMappingDir, "ACCEPTANCE.json"), "utf8"));
if (manifest.forbidden_content_included !== false || manifest.l3_included !== false || acceptance.status !== "passed") {
  fail("public mapping release is not accepted for publication");
}

const activeManifest = JSON.parse(fs.readFileSync(path.join(runtimeRoot, "active_manifest.json"), "utf8"));
const jobId = String(activeManifest.job_id || "");
const runtimeDirs = fs.readdirSync(runtimeRoot, { withFileTypes: true }).filter((entry) => entry.isDirectory());
if (!jobId.startsWith("lcia-official-") || runtimeDirs.length !== 1 || runtimeDirs[0].name !== jobId) {
  fail("release must contain exactly the active official EF3.1 runtime");
}

process.stdout.write(JSON.stringify({
  status: "ok",
  publicMappingVersion: manifest.dataset_version,
  officialRuntime: jobId,
  firstPartyFileCount: firstPartyFiles.length,
}) + "\n");
