import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5193,
    // The API's real port lives here and nowhere else - no hardcoded
    // http://localhost:8700 anywhere in src/.
    proxy: {
      "/payments": "http://localhost:8700",
      "/kafka": "http://localhost:8700",
    },
  },
});
