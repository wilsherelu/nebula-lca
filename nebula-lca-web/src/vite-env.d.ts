/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_NEBULA_ONE_RETURN_ORIGIN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
