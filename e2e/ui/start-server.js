// Starts datafeedfs-go for the browser tests against a throw-away config and cache, and keeps running until it is stopped.
//   node start-server.js edit      mount + console, editing on (port 18088); falls back to `serve` when FUSE is not usable
//   node start-server.js readonly  console only, editing off, token required (port 18089)
// It writes .state/<name>.json (config path, cache dir, whether a mount exists) for the tests and removes everything on exit.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn, spawnSync } = require('child_process');

const name = process.argv[2];
const repo = path.resolve(__dirname, '..', '..');
const binary = process.env.DFS_BIN || path.join(__dirname, 'bin', 'datafeedfs-go');
if (!fs.existsSync(binary)) {
  console.error(`${binary} is missing: run "npm run build" (or "npm test") first`);
  process.exit(1);
}
const settings = {
  edit: { port: 18088, token: '', allowEdit: true, wantMount: true },
  readonly: { port: 18089, token: 's3cret', allowEdit: false, wantMount: false },
}[name];
if (!settings) throw new Error('usage: start-server.js edit|readonly');

function fuseUsable() {
  try {
    fs.accessSync('/dev/fuse', fs.constants.R_OK | fs.constants.W_OK);
  } catch {
    return false;
  }
  return ['fusermount3', 'fusermount'].some((tool) => spawnSync('sh', ['-c', `command -v ${tool}`]).status === 0);
}

const work = fs.mkdtempSync(path.join(os.tmpdir(), `dfs-ui-${name}-`));
const cache = path.join(work, 'cache');
const mountPath = path.join(work, 'mnt');
const configPath = path.join(work, 'config.toml');
const mounted = settings.wantMount && fuseUsable();
fs.writeFileSync(
  configPath,
  `# config of the browser tests
[remote]
ssh_host = ""
pcap_path = "${repo}/testdata/pcaps/{yyyy}/{mm}/{dd}/nasdaq_tv.pcap.xz"

[parser]
command = "${repo}/parsers/parse_nasdaq_itch.sh"
env = { DFS_PORT = "26477" }

[cache]
dir = "${cache}"
scan_interval_seconds = 1

[extract]
batch_window_seconds = 0.3
max_parallel = 2

[ui]
listen = "127.0.0.1:${settings.port}"
allow_edit = ${settings.allowEdit}
${settings.token ? `token = "${settings.token}"\n` : ''}`
);
fs.mkdirSync(path.join(__dirname, '.state'), { recursive: true });
fs.writeFileSync(path.join(__dirname, '.state', `${name}.json`), JSON.stringify({ configPath, cache, mountPath, mounted, work }));

const args = mounted ? ['mount', configPath, mountPath] : ['serve', configPath];
console.log(`[${name}] ${binary} ${args.join(' ')}  (${mounted ? 'mounted' : 'no mount: FUSE not usable or not wanted'})`);
const child = spawn(binary, args, { stdio: 'inherit', env: { ...process.env, PATH: `${path.dirname(binary)}:${process.env.PATH}` } });
let stopping = false;
function stop() {
  if (stopping) return;
  stopping = true;
  child.kill('SIGTERM');
  setTimeout(() => child.kill('SIGKILL'), 8000).unref();
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
child.on('exit', () => {
  if (mounted) spawnSync('fusermount', ['-uz', mountPath]);
  fs.rmSync(work, { recursive: true, force: true });
  process.exit(0);
});
