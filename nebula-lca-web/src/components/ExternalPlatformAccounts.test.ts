import { describe, expect, it } from "vitest";
import { importedProjectTarget } from "./ExternalPlatformAccounts";

describe("importedProjectTarget", () => {
  it("uses the model sync response project id", () => {
    expect(
      importedProjectTarget(
        {
          job_id: "job-1",
          status: "completed",
          project_id: "project-1",
          tidas_import_report: { created_projects: [{ name: "Imported model", version: 1 }] },
        },
        "Fallback name",
      ),
    ).toEqual({ projectId: "project-1", projectName: "Imported model" });
  });

  it("falls back to the created project record", () => {
    expect(
      importedProjectTarget({
        job_id: "job-2",
        status: "completed",
        tidas_import_report: { created_projects: [{ project_id: "project-2" }] },
      }),
    ).toEqual({ projectId: "project-2", projectName: undefined });
  });
});
