import { terser } from "rollup-plugin-terser";

export default {
  input: "dist/embedded/global.js",
  output: {
    file: "bundle/embedded.min.js",
    format: "iife",
  },
  plugins: [
    terser({ mangle: false }),
  ],
};
