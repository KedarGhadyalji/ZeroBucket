import { main } from "./cli.js";

main(process.argv.slice(2)).then(
  (code) => {
    process.exitCode = code;
  },
  (exc) => {
    console.error(`Error: ${exc instanceof Error ? exc.message : String(exc)}`);
    process.exitCode = 1;
  },
);
