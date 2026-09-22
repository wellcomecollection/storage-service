import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";

export default defineConfig([
  ...nextVitals,
  globalIgnores([".next/**", "out/**", "next-env.d.ts"]),
  {
    rules: {
      // New in eslint-config-next 16; existing mount-time localStorage and prop-sync effects trip it.
      "react-hooks/set-state-in-effect": "warn",
    },
  },
]);
