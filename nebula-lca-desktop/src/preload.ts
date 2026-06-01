import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("__NEBULA_DESKTOP__", {
  apiBase: process.env.NEBULA_API_BASE,
  openLogs: () => ipcRenderer.invoke("desktop:openLogs"),
  getDiagnostics: () => ipcRenderer.invoke("desktop:getDiagnostics"),
  chooseImportFile: () => ipcRenderer.invoke("desktop:chooseImportFile"),
});
