import { contextBridge, ipcRenderer } from "electron";

let apiBase = process.env.NEBULA_API_BASE;
ipcRenderer.on("set-api-base", (_event, url: string) => {
  apiBase = url;
});

contextBridge.exposeInMainWorld("__NEBULA_DESKTOP__", {
  get apiBase() {
    return apiBase;
  },
  openLogs: () => ipcRenderer.invoke("desktop:openLogs"),
  getDiagnostics: () => ipcRenderer.invoke("desktop:getDiagnostics"),
  chooseImportFile: () => ipcRenderer.invoke("desktop:chooseImportFile"),
});
