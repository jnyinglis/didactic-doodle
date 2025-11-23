import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  root: __dirname,
  base:
    process.env.GITHUB_REPOSITORY?.split("/")[1]
      ? `/${process.env.GITHUB_REPOSITORY.split("/")[1]}/`
      : "/",
  resolve: {
    alias: {
      "@engine": path.resolve(__dirname, "../src"),
    },
  },
  server: {
    port: 5173,
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
