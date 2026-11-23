// Bootstrap with the shell's Node, but run frontend tools with this project's
// Node 24. Never change the user's global runtime, PATH, or other processes.
import { existsSync } from 'node:fs';
import { spawn, execFileSync } from 'node:child_process';
import { dirname, delimiter, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const projectNode = resolve(root, '.tooling/node_modules/node/bin/node');
const runtime = existsSync(projectNode) ? projectNode : process.execPath;
const version = execFileSync(runtime, ['--version'], { encoding: 'utf8' }).trim();
if (!version.startsWith('v24.')) {
  console.error('This frontend uses Node 24. Follow the project-local setup in README.md; no global Node change is needed.');
  process.exit(1);
}
const tools = {
  dev: [['vite/bin/vite.js']],
  preview: [['vite/bin/vite.js', 'preview']],
  typecheck: [['typescript/bin/tsc', '--noEmit']],
  build: [['typescript/bin/tsc', '--noEmit'], ['vite/bin/vite.js', 'build']],
  test: [['vitest/vitest.mjs', 'run']],
  'test:watch': [['vitest/vitest.mjs']],
};
const command = process.argv[2];
if (!Object.hasOwn(tools, command)) { console.error('Unknown frontend command.'); process.exit(1); }

let child;
let stopped = false;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { stopped = true; child?.kill(signal); });
for (const [index, [entry, ...args]] of tools[command].entries()) {
  const code = await new Promise(resolveExit => {
    const extra = index === tools[command].length - 1 ? process.argv.slice(3) : [];
    child = spawn(runtime, [resolve(root, 'node_modules', entry), ...args, ...extra], {
      cwd: root,
      stdio: 'inherit',
      env: { ...process.env, PATH: `${dirname(runtime)}${delimiter}${process.env.PATH || ''}` },
    });
    child.once('error', error => { console.error(error.message); resolveExit(1); });
    child.once('exit', (status, signal) => resolveExit(status ?? (signal === 'SIGINT' ? 130 : 1)));
  });
  child = undefined;
  if (code || stopped) { process.exitCode = code || 130; break; }
}
