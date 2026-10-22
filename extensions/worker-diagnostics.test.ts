import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { EventEmitter } from "node:events";
import { expect } from "chai";
import { SerenaWorkerClient } from "./worker";
import serenaToolsExtension from "./index";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// Exercise ensureStarted and its real stream/lifecycle handlers, not a copy.
async function withWorker(run: (worker: SerenaWorkerClient, procs: any[]) => Promise<void>) {
  const saved = { spawn: childProcess.spawn, spawnSync: childProcess.spawnSync,
    python: process.env.SERENA_PYTHON, config: process.env.PI_CODING_AGENT_DIR,
    out: process.stdout.write, err: process.stderr.write };
  const procs: any[] = [];
  const writes: string[] = [];
  let inWorkerEvent = false;
  const stream = () => {
    const emitter = new EventEmitter();
    const emit = emitter.emit;
    emitter.emit = function (...args: Parameters<typeof emit>) {
      inWorkerEvent = true;
      try { return emit.apply(this, args); } finally { inWorkerEvent = false; }
    };
    return emitter;
  };
  process.env.SERENA_PYTHON = process.execPath;
  process.env.PI_CODING_AGENT_DIR = process.cwd(); // Do not read live global dot files.
  childProcess.spawnSync = (() => ({ status: 1 })) as any;
  childProcess.spawn = (() => {
    const proc: any = stream();
    proc.stdout = stream();
    proc.stdout.setEncoding = () => {};
    proc.stderr = stream();
    proc.stderr.setEncoding = () => {};
    proc.stdin = new EventEmitter();
    proc.stdin.write = () => true;
    proc.exitCode = proc.signalCode = null;
    proc.kill = (signal?: string) => { proc.killed = true; proc.killSignal = signal; return true; };
    procs.push(proc);
    return proc;
  }) as any;
  syncBuiltinESMExports();
  // Capture worker event handlers only. Test reporters may flush asynchronously
  // during timeout tests and must not be mistaken for worker terminal writes.
  process.stdout.write = ((...args: any[]) => {
    if (!inWorkerEvent) return saved.out.apply(process.stdout, args as any);
    writes.push(String(args[0])); return true;
  }) as any;
  process.stderr.write = ((...args: any[]) => {
    if (!inWorkerEvent) return saved.err.apply(process.stderr, args as any);
    writes.push(String(args[0])); return true;
  }) as any;
  const worker = new SerenaWorkerClient();
  try {
    await run(worker, procs);
  } finally {
    // Prevent cleanup-triggered queue microtasks from spawning after mocks restore.
    (worker as any).stopping = true;
    (worker as any).rejectQueued(new Error("test cleanup"));
    (worker as any).killAndReset();
    process.stdout.write = saved.out;
    process.stderr.write = saved.err;
    childProcess.spawn = saved.spawn;
    childProcess.spawnSync = saved.spawnSync;
    syncBuiltinESMExports();
    for (const [key, value] of [["SERENA_PYTHON", saved.python], ["PI_CODING_AGENT_DIR", saved.config]]) {
      if (value === undefined) delete process.env[key!]; else process.env[key!] = value;
    }
    expect(writes, "worker diagnostics must not write through Pi's terminal").to.deep.equal([]);
  }
}

function reply(proc: any, response: object) {
  proc.stdout.emit("data", JSON.stringify(response) + "\n");
}

// Disposable OS children: the import/delay modes execute the actual embedded bridge.
async function withRealWorker(mode: string, run: (worker: SerenaWorkerClient, modeFile: string) => Promise<void>) {
  const python = process.env.SERENA_TEST_PYTHON!;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "serena-lifecycle-"));
  const saved = { python: process.env.SERENA_PYTHON, config: process.env.PI_CODING_AGENT_DIR };
  const modeFile = path.join(dir, "mode");
  fs.writeFileSync(modeFile, mode);
  const wrapper = path.join(dir, "python-wrapper.py");
  fs.writeFileSync(wrapper, `#!${python}
import json, os, pathlib, sys, time
mode = pathlib.Path(__file__).with_name('mode').read_text()
if mode == 'import-fail':
    # Real missing installation: -S removes site-packages, with no import mock.
    os.execv(sys.executable, [sys.executable, '-S', '-u', '-c', sys.argv[-1]])
if mode == 'delay': time.sleep(0.25)
if mode == 'lifecycle':
    for line in sys.stdin:
        req = json.loads(line)
        action = req.get('action')
        if action == 'hang':
            print('HANG_READY', file=sys.stderr, flush=True)
            time.sleep(60)
        if action == 'silent-hang': time.sleep(60)
        if action == 'exit': os._exit(7)
        print(json.dumps({'id': req['id'], 'ok': True, 'pid': os.getpid()}), flush=True)
        if action == 'warn': print('STALE_PREVIOUS_CALL', file=sys.stderr, flush=True)
        if action == 'shutdown': break
else:
    exec(compile(sys.argv[-1], '<actual-serena-bridge>', 'exec'))
`, { mode: 0o700 });
  process.env.SERENA_PYTHON = wrapper;
  process.env.PI_CODING_AGENT_DIR = dir;
  const worker = new SerenaWorkerClient();
  try { await run(worker, modeFile); }
  finally {
    await worker.stop();
    for (const [key, value] of [["SERENA_PYTHON", saved.python], ["PI_CODING_AGENT_DIR", saved.config]]) {
      if (value === undefined) delete process.env[key!]; else process.env[key!] = value;
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe("terminal-safe worker diagnostics", () => {
  for (const failure of ["Serena worker restarted", "Serena worker exited code=7 signal=null", "Serena worker exited code=null signal=SIGKILL", "Serena worker process error: broken pipe"]) {
    it(`warns through the registered mutation boundary on ${failure}`, async () => {
      const request = SerenaWorkerClient.prototype.request;
      let calls = 0;
      SerenaWorkerClient.prototype.request = async () => { calls++; throw new Error(failure); };
      try {
        const tools: any[] = [];
        serenaToolsExtension({ on() {}, registerTool(tool: any) { tools.push(tool); }, registerCommand() {} } as any);
        const error = await tools.find(t => t.name === "serena_replace_content").execute("test", {
          relative_path: "sample.ts", needle: "old", repl: "new", mode: "literal",
        }, undefined, undefined, {}).catch((error: Error) => error);
        expect(error.message).to.include(failure).and.include("may have partially applied").and.include("verify state before retrying");
        expect(calls, "mutations must not be replayed").to.equal(1);
      } finally { SerenaWorkerClient.prototype.request = request; }
    });
  }

  it("marks structured failures as Pi errors without matching successful source content", async () => {
    const request = SerenaWorkerClient.prototype.request;
    let fail = true;
    SerenaWorkerClient.prototype.request = async () => fail
      ? { id: "1", ok: false, error: "ToolCallError: Tool 'find_symbol' is not active", errorType: "inactive_tool" }
      : { id: "2", ok: true, result: "Error: Tool 'example' is not active. Ordinary source content" };
    try {
      const tools: any[] = [];
      serenaToolsExtension({ on() {}, registerTool(tool: any) { tools.push(tool); }, registerCommand() {} } as any);
      const tool = tools.find(t => t.name === "serena_find_symbol");
      const failure = await tool.execute("test", {}, undefined, undefined, {});
      expect(failure.isError).to.equal(true);
      expect(failure.details.ok).to.equal(false);
      fail = false;
      const success = await tool.execute("test", {}, undefined, undefined, {});
      expect(success.isError).to.equal(false);
      expect(success.details.ok).to.equal(true);
      expect(success.content[0].text).to.include("Ordinary source content");
    } finally { SerenaWorkerClient.prototype.request = request; }
  });

  it("reports a superseded concurrent registered restart as failure, never early readiness", async () => {
    await withWorker(async (_worker, procs) => {
      const tools: any[] = [], shutdown: any[] = [];
      serenaToolsExtension({
        on(event: string, fn: any) { if (event === "session_shutdown") shutdown.push(fn); },
        registerTool(tool: any) { tools.push(tool); }, registerCommand() {},
      } as any);
      const restart = tools.find(t => t.name === "serena_restart_worker");
      const ctx = { ui: {} };
      try {
        const first = restart.execute("first", {}, undefined, undefined, ctx);
        const second = restart.execute("second", {}, undefined, undefined, ctx);
        expect(procs).to.have.length(2);
        const superseded = await first;
        expect(superseded.details.ok).to.equal(false);
        expect(superseded.content[0].text).to.include("Serena worker restarted");
        let settled = false;
        second.then(() => { settled = true; });
        await delay(0);
        expect(settled).to.equal(false);
        reply(procs[1], { id: "2", ok: true });
        expect((await second).details.ok).to.equal(true);
      } finally {
        const stopping = Promise.all(shutdown.map(fn => fn()));
        reply(procs.at(-1), { id: "3", ok: true });
        await stopping;
      }
    });
  });
  it("waits for a real child status response before reporting restart readiness", async function () {
    if (!process.env.SERENA_TEST_PYTHON) this.skip();
    await withRealWorker("delay", async worker => {
      let settled = false;
      const restarted = Promise.resolve(worker.restart()).then(response => { settled = true; return response; });
      await delay(50);
      expect(settled, "restart must wait for the bridge status round-trip").to.equal(false);
      expect((await restarted)?.ok).to.equal(true);
    });
  });

  it("does not replay a real restart import failure to a later recovered caller", async function () {
    if (!process.env.SERENA_TEST_PYTHON) this.skip();
    await withRealWorker("import-fail", async (worker, modeFile) => {
      const restarted = Promise.resolve(worker.restart());
      const proc = (worker as any).process;
      await new Promise(resolve => proc.once("close", resolve));
      const failure = await restarted;
      fs.writeFileSync(modeFile, "healthy");
      const fresh = await worker.request({ action: "status" }, 5000);
      expect(fresh.ok, "a recovered caller must start its own generation").to.equal(true);
      expect(failure?.ok).to.equal(false);
      expect(failure?.error).to.include("ModuleNotFoundError: No module named 'serena'");
      expect(failure?.error).to.include("Traceback");
      expect(fresh.diagnostics).to.equal(undefined);
    });
  });

  it("settles a real SIGKILL without replaying the in-flight mutation", async function () {
    if (!process.env.SERENA_TEST_PYTHON) this.skip();
    await withRealWorker("lifecycle", async worker => {
      const first = await worker.request({ action: "status" }, 5000);
      const proc = (worker as any).process;
      const ready = new Promise(resolve => proc.stderr.once("data", resolve));
      const failed = worker.request({ action: "hang" }, 5000).catch(error => error);
      const next = worker.request({ action: "status" }, 5000);
      await ready;
      proc.kill("SIGKILL");
      expect((await failed).message).to.include("signal=SIGKILL");
      const fresh = await next;
      expect(fresh.ok).to.equal(true);
      expect(fresh.pid).not.to.equal(first.pid);
      expect(fresh.diagnostics).to.equal(undefined);
    });
  });

  for (const action of ["silent-hang", "exit"]) {
    it(`labels real stale stderr as uncorrelated on ${action}`, async function () {
      if (!process.env.SERENA_TEST_PYTHON) this.skip();
      await withRealWorker("lifecycle", async worker => {
        await worker.request({ action: "status" }, 5000);
        const raw = new Promise(resolve => (worker as any).process.stderr.once("data", resolve));
        await worker.request({ action: "warn" }, 5000);
        await raw; // Prove the old output arrived before the next request.
        const failure = await worker.request({ action }, 100).catch(error => error);
        expect(failure.message).to.include("Uncorrelated worker output (tail; not request-specific)");
        expect(failure.message).to.include("STALE_PREVIOUS_CALL");
        expect(failure.message).not.to.include("Worker diagnostics (tail)");
      });
    });
  }

  it("reports real import failure through both registered restart interfaces", async function () {
    if (!process.env.SERENA_TEST_PYTHON) this.skip();
    await withRealWorker("import-fail", async () => {
      const tools: any[] = [], commands: any[] = [], shutdown: any[] = [], notifications: any[] = [];
      serenaToolsExtension({
        on(event: string, fn: any) { if (event === "session_shutdown") shutdown.push(fn); },
        registerTool(tool: any) { tools.push(tool); },
        registerCommand(name: string, command: any) { commands.push({ name, ...command }); },
      } as any);
      const ctx = { ui: { notify: (...args: any[]) => notifications.push(args) } };
      try {
        const response = await tools.find(t => t.name === "serena_restart_worker").execute("test", {}, undefined, undefined, ctx);
        expect(response.details.ok).to.equal(false);
        expect(response.content[0].text).to.include("No module named 'serena'");
        await commands.find(c => c.name === "serena-restart").handler("", ctx);
        expect(notifications.at(-1)[0]).to.include("No module named 'serena'");
        expect(notifications.at(-1)[1]).to.equal("error");
      } finally { for (const fn of shutdown) await fn(); }
    });
  });
  it("propagates list-tools failures instead of inventing an empty tool list", async () => {
    const request = SerenaWorkerClient.prototype.request;
    let calls = 0;
    SerenaWorkerClient.prototype.request = async () => {
      calls++;
      return { id: "1", ok: false, error: "ValueError: broken context", errorType: "serena_error" };
    };
    try {
      const tools: any[] = [];
      serenaToolsExtension({ on() {}, registerTool(tool: any) { tools.push(tool); }, registerCommand() {} } as any);
      const result = await tools.find(t => t.name === "serena_list_tools").execute("test", {}, undefined, undefined, {});
      expect(result.content[0].text).to.include("ValueError: broken context");
      expect(result.content[0].text).not.to.include("No active tools");
      expect(result.details.ok).to.equal(false);
      expect(calls).to.equal(1);
    } finally { SerenaWorkerClient.prototype.request = request; }
  });

  it("keeps a structured traceback with an error-first reply and later successes clean", async () => {
    await withWorker(async (worker, procs) => {
      const first = worker.request({ action: "replace_content" });
      reply(procs[0], { id: "1", ok: false, error: "ValueError: broken", diagnostics: "Traceback: request one" });
      // Separate pipes may deliver stderr after the JSON error, even during call 2.
      const failure = await first;
      const second = worker.request({ action: "find_symbol" });
      procs[0].stderr.emit("data", "late unrelated stderr from request one\n");
      reply(procs[0], { id: "2", ok: true, result: "clean" });
      expect(failure.error).to.include("Traceback: request one");
      expect((await second).diagnostics).to.equal(undefined);
      const third = worker.request({ action: "find_symbol" });
      reply(procs[0], { id: "3", ok: true, result: "still clean" });
      expect((await third).diagnostics).to.equal(undefined);
    });
  });

  it("runs the actual Python bridge with current and legacy backends, exceptions and clean recovery", async function () {
    const python = process.env.SERENA_TEST_PYTHON;
    if (!python) this.skip();
    this.timeout(20_000);
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "serena-bridge-review-"));
    const saved = { python: process.env.SERENA_PYTHON, config: process.env.PI_CODING_AGENT_DIR };
    const wrapper = path.join(dir, "python-wrapper.py");
    // Real installed Serena imports and current backend class; only agent setup is
    // replaced to avoid dashboard, project/config writes and language-server startup.
    fs.writeFileSync(wrapper, `#!${python}
import sys
from types import SimpleNamespace
import serena.agent
from serena.lsp.lsp_backend import LSPLanguageBackend
from serena.config.context_mode import SerenaAgentContext
from serena.config.serena_config import SerenaConfig
SerenaAgentContext.load = staticmethod(lambda context: context)
SerenaConfig.from_config_file = staticmethod(lambda: SimpleNamespace())
class Agent:
    def __init__(self, project, context, serena_config):
        if context == 'broken':
            raise ValueError('actual bridge failure')
        self.backend = SimpleNamespace(value='LSP') if context == 'legacy' else LSPLanguageBackend()
    def get_language_backend(self): return self.backend
    def get_active_tool_names(self): return ['find_symbol']
    def get_active_project(self): return None
    def get_dashboard_url(self): return None
    def on_shutdown(self): pass
serena.agent.SerenaAgent = Agent
exec(compile(sys.argv[-1], '<actual-serena-bridge>', 'exec'))
`, { mode: 0o700 });
    process.env.SERENA_PYTHON = wrapper;
    process.env.PI_CODING_AGENT_DIR = dir;
    const worker = new SerenaWorkerClient();
    try {
      const modern = await worker.request({ action: "status", context: "modern", includeAgent: true }, 15_000);
      expect(modern.ok, String(modern.error)).to.equal(true);
      expect(modern.languageBackend).to.equal("LSPLanguageBackend");
      const legacy = await worker.request({ action: "status", context: "legacy", includeAgent: true });
      expect(legacy.languageBackend).to.equal("LSP");
      const failure = await worker.request({ action: "status", context: "broken", includeAgent: true });
      expect(failure.ok).to.equal(false);
      expect(failure.error).to.include("ValueError: actual bridge failure").and.include("Traceback");
      expect(failure.diagnostics).to.include("<actual-serena-bridge>");
      expect(failure.error).not.to.include("serena_restart_worker");
      const clean = await worker.request({ action: "status", context: "modern", includeAgent: true });
      expect(clean.ok).to.equal(true);
      expect(clean.diagnostics).to.equal(undefined);
    } finally {
      await worker.stop();
      for (const [key, value] of [["SERENA_PYTHON", saved.python], ["PI_CODING_AGENT_DIR", saved.config]]) {
        if (value === undefined) delete process.env[key!]; else process.env[key!] = value;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
  it("keeps stderr and invalid stdout off the terminal while preserving successful queueing", async () => {
    await withWorker(async (worker, procs) => {
      const first = worker.request({ action: "find_symbol" });
      const second = worker.request({ action: "find_symbol" });
      procs[0].stderr.emit("data", "Python warning\n");
      procs[0].stdout.emit("data", "non-json warning\n");
      reply(procs[0], { id: "1", ok: true, result: "one" });
      const response = await first;
      expect(response.result).to.equal("one");
      expect(response.diagnostics).to.equal(undefined);
      reply(procs[0], { id: "2", ok: true, result: "two" });
      const later = await second;
      expect(later.result).to.equal("two");
      expect(later.diagnostics).to.equal(undefined);
    });
  });

  it("bounds same-protocol startup diagnostics and keeps the next generation clean", async () => {
    await withWorker(async (worker, procs) => {
      const first = worker.request({ action: "replace_content" });
      reply(procs[0], { id: null, ok: false, error: "ImportError: broken", diagnostics: "x".repeat(20_000) + "startup trace" });
      const failed = await first;
      expect(failed.diagnostics).to.have.length(8192);
      expect(failed.error).to.include("startup trace");
      expect(failed.error).not.to.include("serena_restart_worker");
      const next = worker.request({ action: "find_symbol" });
      reply(procs[1], { id: "2", ok: true, result: "clean" });
      expect((await next).diagnostics).to.equal(undefined);
    });
  });

  it("returns no-id startup failures promptly with diagnostics and clears the pending timer", async () => {
    await withWorker(async (worker, procs) => {
      const result = worker.request({ action: "find_symbol" }, 30_000);
      procs[0].stderr.emit("data", "Traceback: import failed\n");
      reply(procs[0], { id: null, ok: false, error: "ImportError: missing Serena", errorType: "startup_error" });
      let timer: ReturnType<typeof setTimeout>;
      try {
        const response = await Promise.race([result, new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("startup failure waited instead of settling")), 100);
        })]);
        expect(response.ok).to.equal(false);
        expect(response.error).to.include("missing Serena").and.include("Traceback");
        expect(response.error).not.to.include("serena_restart_worker");
        expect(response.errorType).to.equal("startup_error");
        expect(procs[0].killed).to.equal(true);
        expect(procs[0].killSignal).to.equal("SIGKILL");
        expect((worker as any).pending.size).to.equal(0);
      } finally { clearTimeout(timer!); }
    });
  });

  it("bounds diagnostics, includes them on exit, and ignores old-generation streams", async () => {
    await withWorker(async (worker, procs) => {
      const failed = worker.request({ action: "find_symbol" }).catch((error) => error);
      procs[0].stderr.emit("data", "x".repeat(30_000) + "useful tail");
      expect((worker as any).diagnostics.length).to.be.at.most(8192);
      procs[0].emit("close", 1, null);
      expect((await failed).message).to.include("useful tail");
      const next = worker.request({ action: "find_symbol" });
      procs[0].stderr.emit("data", "old warning");
      reply(procs[0], { ok: false, error: "old startup error" });
      reply(procs[1], { id: "2", ok: true, result: "fresh" });
      expect((await next).result).to.equal("fresh");
      expect((worker as any).diagnostics).not.to.include("old warning");
    });
  });

  it("returns startup failure to restart itself and never replays it to a later call", async () => {
    await withWorker(async (worker, procs) => {
      const restarted = worker.restart();
      reply(procs[0], { ok: false, error: "Failed to import Serena" });
      const response = await restarted;
      expect(response.ok).to.equal(false);
      expect(response.error).to.include("Failed to import Serena");
      expect(response.errorType).to.equal("worker_startup_error");
      const fresh = worker.request({ action: "replace_content" }, 50);
      expect(procs.length).to.equal(2);
      reply(procs[1], { id: "2", ok: true, result: "applied once" });
      expect((await fresh).result).to.equal("applied once");
    });
  });

  it("starts the next queued request in a clean generation after a startup failure", async () => {
    await withWorker(async (worker, procs) => {
      const first = worker.request({ action: "replace_content" }, 50);
      const second = worker.request({ action: "find_symbol" }, 50);
      reply(procs[0], { ok: false, error: "Failed to import Serena" });
      expect((await first).ok).to.equal(false);
      expect(procs.length).to.equal(2);
      reply(procs[0], { id: "2", ok: true, result: "stale" });
      reply(procs[1], { id: "2", ok: true, result: "fresh" });
      expect((await second).result).to.equal("fresh");
    });
  });

  it("includes the diagnostic tail on timeout and leaves a fresh generation clean", async () => {
    await withWorker(async (worker, procs) => {
      const first = worker.request({ action: "replace_content" }, 5).catch(error => error);
      procs[0].stderr.emit("data", "timeout trace");
      const failure = await first;
      expect(failure.message).to.include("timeout trace").and.include("verify state before retrying");
      expect(failure.message).to.include("the next call will start a fresh bridge");
      expect(failure.message).not.to.include("Worker has been restarted");
      const next = worker.request({ action: "find_symbol" }, 50);
      reply(procs[1], { id: "2", ok: true, result: "fresh" });
      expect((await next).diagnostics).to.equal(undefined);
    });
  });

  it("adds a conditional restart hint to resource errors without retrying or relabelling configuration", async () => {
    await withWorker(async (worker, procs) => {
      const result = worker.request({ action: "replace_symbol_body" });
      reply(procs[0], { id: "1", ok: false, errorType: "serena_error",
        error: "Context ide not found in /old/serena/resources/config/contexts. Available contexts: []" });
      const response = await result;
      expect(response.error).to.include("serena_restart_worker").and.include("configuration");
      expect(response.errorType).to.equal("serena_error");
      expect(procs.length).to.equal(1);
      expect(procs[0].killed).not.to.equal(true);
    });
  });
});
