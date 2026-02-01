import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

type HookHandler = (event: Record<string, unknown>) => void | Promise<void>;
type HookRegistrar = (event: string, handler: HookHandler, name: string) => void;

/**
 * OpenClaw Plugin API interface
 * Provided by OpenClaw when the plugin is loaded
 */
export interface OpenClawPluginApi {
  pluginConfig?: Record<string, unknown>;
  logger: {
    info?: (msg: string) => void;
    warn?: (msg: string) => void;
    error?: (msg: string) => void;
    debug?: (msg: string) => void;
  };
  callGateway?: unknown;
  registerHook?: (opts: { event: string; handler: HookHandler }) => void;
  hooks?: {
    register?: (opts: { event: string; handler: HookHandler }) => void;
    on?: (event: string, handler: HookHandler) => void;
  };
  registerHttpRoute: (opts: {
    path: string;
    handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>;
  }) => void;
}

// Gateway call function type - loaded dynamically
type CallGateway = (opts: {
  method: string;
  params?: unknown;
  expectFinal?: boolean;
  timeoutMs?: number;
}) => Promise<unknown>;

type LinearSessionState = {
  sessionId: string;
  heartbeat?: ReturnType<typeof setInterval>;
};

const callRef: { value?: CallGateway } = {};
const viewerRef: { value?: string } = {};
const warnRef = { value: false };
const stateRef: Record<string, string> = {};
const sessionRef: Record<string, LinearSessionState> = {};
const sessionIdRef: Record<string, string> = {};
const hookStateRef = { registered: false, warned: false };

const MAX_BODY = 2 * 1024 * 1024;
const AGENT_TIMEOUT_MS = 30 * 60 * 1000;
const LINEAR_API_URL = "https://api.linear.app/graphql";

const ACTIVITY_MUTATION = `
  mutation AgentActivityCreate($input: AgentActivityCreateInput!) {
    agentActivityCreate(input: $input) {
      success
      agentActivity { id }
    }
  }
`;

const SESSION_UPDATE_MUTATION = `
  mutation AgentSessionUpdate($id: String!, $input: AgentSessionUpdateInput!) {
    agentSessionUpdate(id: $id, input: $input) { success }
  }
`;

const ISSUE_INFO_QUERY = `
  query IssueInfo($id: String!) {
    issue(id: $id) {
      id
      state { type }
      team { id }
      delegate { id }
    }
  }
`;

const TEAM_STARTED_QUERY = `
  query TeamStartedStates($id: String!) {
    team(id: $id) {
      states(filter: { type: { eq: "started" } }) {
        nodes { id position }
      }
    }
  }
`;

const ISSUE_UPDATE_MUTATION = `
  mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
    issueUpdate(id: $id, input: $input) { success }
  }
`;

const VIEWER_QUERY = `query Viewer { viewer { id } }`;

type LinearCfg = {
  devAgentId?: string;
  linearWebhookSecret?: string;
  linearApiKey?: string;
  notifyChannel?: string;
  notifyTo?: string;
  notifyAccountId?: string;
  repoByTeam?: Record<string, string>;
  repoByProject?: Record<string, string>;
  defaultDir?: string;
  delegateOnCreate?: boolean;
  startOnCreate?: boolean;
  externalUrlBase?: string;
  externalUrlLabel?: string;
  streamActivities?: boolean;
  streamToolCalls?: boolean;
  streamIntervalMs?: number;
  streamMaxChars?: number;
  streamToolAllowlist?: string[];
  streamToolDenylist?: string[];
};

type ActivityContent =
  | { type: "thought"; body: string }
  | { type: "elicitation"; body: string }
  | { type: "response"; body: string }
  | { type: "error"; body: string }
  | { type: "action"; action: string; parameter: string; result?: string };

type LinearResult = { ok: true; data: Record<string, unknown> } | { ok: false };

type IssueInfo = {
  id: string;
  teamId: string;
  stateType: string;
  delegateId: string;
};

export function createLinearWebhook(api: OpenClawPluginApi) {
  return async (req: IncomingMessage, res: ServerResponse) => {
    if (req.method !== "POST") {
      res.statusCode = 405;
      res.setHeader("Allow", "POST");
      res.end("Method Not Allowed");
      return;
    }

    const read = await readBody(req, MAX_BODY);
    if (!read.ok) {
      res.statusCode = read.status;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(JSON.stringify({ ok: false, error: read.error }));
      return;
    }

    const raw = read.body;
    const cfg = normalizeCfg(api.pluginConfig);
    const secret = cfg.linearWebhookSecret;
    const sig = readHeader(req, "linear-signature");
    const delivery = readHeader(req, "linear-delivery");

    if (secret && !verifySignature(secret, sig, raw)) {
      res.statusCode = 401;
      res.end("Unauthorized");
      return;
    }

    const text = raw.toString("utf8");
    const data = JSON.parse(text) as Record<string, unknown>;
    const stamp =
      typeof data.webhookTimestamp === "number"
        ? data.webhookTimestamp
        : undefined;

    // Reject stale webhooks (> 60 seconds old)
    if (stamp && Math.abs(Date.now() - stamp) > 60_000) {
      res.statusCode = 401;
      res.end("Stale webhook");
      return;
    }

    // Respond immediately, process asynchronously
    res.statusCode = 202;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ ok: true }));

    queueMicrotask(() => {
      void handleWebhook(api, cfg, data, delivery);
    });
  };
}

export function registerLinearHooks(api: OpenClawPluginApi) {
  if (hookStateRef.registered) {
    return;
  }
  hookStateRef.registered = true;

  const registrar = resolveHookRegistrar(api);
  if (!registrar) {
    if (!hookStateRef.warned) {
      hookStateRef.warned = true;
      api.logger.warn?.(
        "Linear plugin: hook API unavailable; streaming activities disabled.",
      );
    }
    return;
  }

  registrar(
    "after_tool_call",
    (event: Record<string, unknown>) => {
      void handleToolHook(api, event);
    },
    "linear-stream-after-tool",
  );

  registrar(
    "before_tool_call",
    (event: Record<string, unknown>) => {
      void handleToolHook(api, event);
    },
    "linear-stream-before-tool",
  );

  registrar(
    "agent_end",
    (event: Record<string, unknown>) => {
      const sessionKey = resolveHookSessionKey(event);
      if (sessionKey) {
        clearLinearSession(sessionKey);
      }
    },
    "linear-stream-agent-end",
  );
}

async function handleWebhook(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  data: Record<string, unknown>,
  delivery: string | undefined,
) {
  const kind = readString(data.type) ?? "";

  if (kind === "PermissionChange" || kind === "OAuthApp") {
    logEvent(api, "permission", data);
    return;
  }

  if (kind === "AppUserNotification") {
    logEvent(api, "notification", data);
    return;
  }

  const session = readObject(data.agentSession);
  if (!session) {
    if (kind) {
      api.logger.info?.(`linear webhook ignored (${kind})`);
    }
    return;
  }

  await handleAgentEvent(api, cfg, data, delivery);
}

async function handleAgentEvent(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  data: Record<string, unknown>,
  delivery: string | undefined,
) {
  const action = resolveAction(data);
  if (!action) {
    api.logger.info?.("linear agent event ignored");
    return;
  }

  const issue = resolveIssue(data);
  const issueId = readString(issue?.id) ?? "";
  const id = readString(issue?.identifier) ?? "";
  const title = readString(issue?.title) ?? "";
  const url = readString(issue?.url) ?? "";
  const desc = readString(issue?.description) ?? "";
  const guidance = readString(data.guidance) ?? "";
  const prompt = resolvePrompt(data);
  const context = resolveContext(data);
  const team = resolveKey(issue?.team);
  const proj = resolveKey(issue?.project);
  const repo = resolveRepo(cfg, team, proj);
  const agent = cfg.devAgentId ?? "dev";
  const label = buildLabel(id, title);
  const session = resolveSessionId(data);
  const key = normalizeKey(session || id || title || randomUUID());
  const sessionKey = `agent:${agent}:linear:${key}`;
  const idem = delivery ?? randomUUID();
  const signal = resolveSignal(data);
  const deliver = Boolean(cfg.notifyChannel && cfg.notifyTo);

  if (resolveFlag(cfg.streamActivities, false) && session) {
    trackLinearSession(api, cfg, sessionKey, session);
  }

  const message = buildMessage({
    action,
    id,
    title,
    url,
    desc,
    guidance,
    prompt,
    repo,
    session,
    context,
  });

  // Handle stop signal
  if (signal === "stop") {
    const text = buildStopText(id, title);
    void postActivity(api, cfg, session, { type: "response", body: text });
    return;
  }

  // Post initial "thinking" activity
  const thought = buildThought(action, id, title);
  void postActivity(
    api,
    cfg,
    session,
    { type: "thought", body: thought },
    { ephemeral: true },
  );

  // Apply issue policies on create
  if (action === "created") {
    const external = resolveExternal(cfg, session, issueId);
    if (external) {
      void updateSessionExternalUrl(
        api,
        cfg,
        session,
        external.url,
        external.label,
      );
    }
    void applyIssuePolicy(api, cfg, issueId);
  }

  // Run the agent and post response
  const call = await loadCallGateway(api);
  try {
    const result = await call({
      method: "agent",
      params: {
        message,
        agentId: agent,
        sessionKey,
        label,
        idempotencyKey: idem,
        deliver,
        channel: cfg.notifyChannel,
        to: cfg.notifyTo,
        accountId: cfg.notifyAccountId,
      },
      expectFinal: true,
      timeoutMs: AGENT_TIMEOUT_MS,
    });

    const text = buildAgentResponse(result);
    // Only post if we have actual content (not the fallback message)
    if (!text || text === "Agent completed with no reply.") {
      return;
    }
    void postActivity(api, cfg, session, { type: "response", body: text });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    api.logger.warn?.(`linear agent run failed: ${msg}`);
    void postActivity(api, cfg, session, {
      type: "error",
      body: `Agent run failed: ${msg}`,
    });
  } finally {
    clearLinearSession(sessionKey);
  }
}

// ==========================================================================
// Streaming hook helpers
// ==========================================================================

function resolveHookRegistrar(api: OpenClawPluginApi): HookRegistrar | null {
  if (typeof api.registerHook === "function") {
    return (event, handler, name) => {
      const registerHook = api.registerHook as unknown as (
        ...args: unknown[]
      ) => void;
      try {
        registerHook(event, handler, name);
      } catch (err) {
        registerHook({ name, event, handler });
      }
    };
  }
  if (typeof api.hooks?.register === "function") {
    return (event, handler, name) => {
      const registerHook = api.hooks?.register as unknown as (
        ...args: unknown[]
      ) => void;
      try {
        registerHook({ name, event, handler });
      } catch (err) {
        registerHook(event, handler, name);
      }
    };
  }
  if (typeof api.hooks?.on === "function") {
    return (event, handler) => api.hooks?.on?.(event, handler);
  }
  return null;
}

async function handleToolHook(
  api: OpenClawPluginApi,
  event: Record<string, unknown>,
) {
  const sessionKey = resolveHookSessionKey(event);
  if (!sessionKey) {
    return;
  }

  const session = sessionRef[sessionKey];
  if (!session) {
    return;
  }

  const cfg = normalizeCfg(api.pluginConfig);
  if (!resolveFlag(cfg.streamActivities, false)) {
    return;
  }
  if (!resolveFlag(cfg.streamToolCalls, true)) {
    return;
  }

  const toolName = resolveHookToolName(event);
  if (!toolName) {
    return;
  }
  if (!shouldStreamTool(cfg, toolName)) {
    return;
  }

  const maxChars = resolveNumber(cfg.streamMaxChars, 500);
  const args = resolveHookToolArgs(event);
  const result = resolveHookToolResult(event);
  const parameter = truncateString(formatStreamValue(args), maxChars);
  const resultText = truncateString(formatStreamValue(result), maxChars);

  const content: ActivityContent = resultText
    ? { type: "action", action: toolName, parameter, result: resultText }
    : { type: "action", action: toolName, parameter };

  void postActivity(api, cfg, session.sessionId, content);
}

function resolveHookSessionKey(event: Record<string, unknown>) {
  const direct = readString(event.sessionKey);
  if (direct) {
    return direct;
  }
  const session = readObject(event.session);
  const fromSession = readString(session?.key);
  if (fromSession) {
    return fromSession;
  }
  const context = readObject(event.context);
  const entry = readObject(context?.sessionEntry);
  const fromContext = readString(entry?.key) ?? readString(context?.sessionKey);
  if (fromContext) {
    return fromContext;
  }
  const sessionId = resolveHookSessionId(event, session, context);
  if (sessionId && sessionIdRef[sessionId]) {
    return sessionIdRef[sessionId];
  }
  return "";
}

function resolveHookSessionId(
  event: Record<string, unknown>,
  session: Record<string, unknown> | undefined,
  context: Record<string, unknown> | undefined,
) {
  return (
    readString(event.sessionId) ??
    readString(event.agentSessionId) ??
    readString(readObject(event.agentSession)?.id) ??
    readString(session?.id) ??
    readString(context?.sessionId) ??
    readString(readObject(context?.agentSession)?.id) ??
    ""
  );
}

function resolveHookToolName(event: Record<string, unknown>) {
  return (
    readString(event.tool) ??
    readString(event.toolName) ??
    readString(readObject(event.toolCall)?.tool) ??
    readString(readObject(event.toolCall)?.name) ??
    readString(readObject(event.tool)?.name) ??
    ""
  );
}

function resolveHookToolArgs(event: Record<string, unknown>) {
  return (
    readObject(event.args) ??
    readObject(event.params) ??
    readObject(readObject(event.toolCall)?.args) ??
    readObject(readObject(event.toolCall)?.params) ??
    readObject(readObject(event.toolCall)?.input) ??
    readObject(readObject(event.tool)?.input) ??
    event.args ??
    event.params
  );
}

function resolveHookToolResult(event: Record<string, unknown>) {
  const error =
    readString(event.error) ??
    readString(readObject(event.error)?.message) ??
    readString(readObject(event.result)?.error) ??
    readString(readObject(readObject(event.result)?.error)?.message);
  if (error) {
    return `Error: ${error}`;
  }

  return (
    event.result ??
    event.output ??
    event.response ??
    readObject(event.toolResult) ??
    readObject(event.resultData)
  );
}

function trackLinearSession(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  sessionKey: string,
  sessionId: string,
) {
  if (!sessionKey || !sessionId) {
    return;
  }

  if (sessionRef[sessionKey]) {
    clearLinearSession(sessionKey);
  }

  const state: LinearSessionState = { sessionId };
  sessionRef[sessionKey] = state;
  sessionIdRef[sessionId] = sessionKey;

  const intervalMs = resolveNumber(cfg.streamIntervalMs, 120000);
  if (intervalMs <= 0) {
    return;
  }

  state.heartbeat = setInterval(() => {
    const current = sessionRef[sessionKey];
    if (!current) {
      return;
    }
    void postActivity(
      api,
      cfg,
      current.sessionId,
      { type: "thought", body: "Working on it..." },
      { ephemeral: true },
    );
  }, intervalMs);
}

function clearLinearSession(sessionKey: string) {
  const current = sessionRef[sessionKey];
  if (!current) {
    return;
  }
  if (current.heartbeat) {
    clearInterval(current.heartbeat);
  }
  delete sessionIdRef[current.sessionId];
  delete sessionRef[sessionKey];
}

function resolveNumber(value: number | undefined, fallback: number) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return fallback;
  }
  return value;
}

function shouldStreamTool(cfg: LinearCfg, toolName: string) {
  const name = toolName.toLowerCase();
  if (cfg.streamToolDenylist?.some((pattern) => matchPattern(name, pattern))) {
    return false;
  }
  if (cfg.streamToolAllowlist && cfg.streamToolAllowlist.length > 0) {
    return cfg.streamToolAllowlist.some((pattern) => matchPattern(name, pattern));
  }
  return true;
}

function matchPattern(value: string, pattern: string) {
  const lowered = pattern.toLowerCase();
  if (lowered === "*") {
    return true;
  }
  if (!lowered.includes("*")) {
    return value === lowered;
  }
  const escaped = lowered.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(`^${escaped.replace(/\\\*/g, ".*")}$`);
  return regex.test(value);
}

function formatStreamValue(value: unknown) {
  if (value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch (err) {
    return String(value);
  }
}

function truncateString(value: string, maxChars: number) {
  if (!value) {
    return "";
  }
  if (value.length <= maxChars) {
    return value;
  }
  if (maxChars <= 3) {
    return value.slice(0, maxChars);
  }
  return `${value.slice(0, maxChars - 3)}...`;
}

async function postActivity(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  session: string,
  content: ActivityContent,
  opts: {
    signal?: string;
    signalMeta?: Record<string, unknown>;
    ephemeral?: boolean;
  } = {},
) {
  if (!session) {
    return;
  }

  const input: Record<string, unknown> = {
    agentSessionId: session,
    content,
  };
  if (opts.signal) {
    input.signal = opts.signal;
  }
  if (opts.signalMeta) {
    input.signalMetadata = opts.signalMeta;
  }
  if (opts.ephemeral) {
    input.ephemeral = true;
  }

  const result = await callLinear(api, cfg, "agentActivityCreate", {
    query: ACTIVITY_MUTATION,
    variables: { input },
  });

  if (!result.ok) {
    return;
  }

  const root = readObject(result.data.agentActivityCreate);
  if (root && root.success === true) {
    return;
  }

  api.logger.warn?.("linear activity failed");
}

async function updateSessionExternalUrl(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  session: string,
  url: string,
  label: string,
) {
  if (!session || !url) {
    return;
  }

  const input = {
    addedExternalUrls: [{ label, url }],
  };

  const result = await callLinear(api, cfg, "agentSessionUpdate", {
    query: SESSION_UPDATE_MUTATION,
    variables: { id: session, input },
  });

  if (!result.ok) {
    return;
  }

  const root = readObject(result.data.agentSessionUpdate);
  if (root && root.success === true) {
    return;
  }

  api.logger.warn?.("linear agentSessionUpdate failed");
}

async function applyIssuePolicy(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  issueId: string,
) {
  const start = resolveFlag(cfg.startOnCreate, true);
  const delegate = resolveFlag(cfg.delegateOnCreate, true);

  if (!issueId) {
    return;
  }
  if (!start && !delegate) {
    return;
  }

  const info = await resolveIssueInfo(api, cfg, issueId);
  if (!info) {
    return;
  }

  if (start) {
    await ensureStarted(api, cfg, info);
  }
  if (delegate) {
    await ensureDelegate(api, cfg, info);
  }
}

async function ensureStarted(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  info: IssueInfo,
) {
  if (!info.teamId) {
    return;
  }

  if (
    info.stateType === "started" ||
    info.stateType === "completed" ||
    info.stateType === "canceled"
  ) {
    return;
  }

  const stateId = await resolveStartedState(api, cfg, info.teamId);
  if (!stateId) {
    return;
  }

  await updateIssue(api, cfg, info.id, { stateId }, "issueUpdate(state)");
}

async function ensureDelegate(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  info: IssueInfo,
) {
  if (info.delegateId) {
    return;
  }

  const viewer = await resolveViewer(api, cfg);
  if (!viewer) {
    return;
  }

  await updateIssue(
    api,
    cfg,
    info.id,
    { delegateId: viewer },
    "issueUpdate(delegate)",
  );
}

async function resolveIssueInfo(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  issueId: string,
): Promise<IssueInfo | null> {
  if (!issueId) {
    return null;
  }

  const result = await callLinear(api, cfg, "issue", {
    query: ISSUE_INFO_QUERY,
    variables: { id: issueId },
  });

  if (!result.ok) {
    return null;
  }

  const issue = readObject(result.data.issue);
  if (!issue) {
    return null;
  }

  const id = readString(issue.id) ?? "";
  if (!id) {
    return null;
  }

  const team = readObject(issue.team);
  const state = readObject(issue.state);
  const delegate = readObject(issue.delegate);

  return {
    id,
    teamId: readString(team?.id) ?? "",
    stateType: readString(state?.type) ?? "",
    delegateId: readString(delegate?.id) ?? "",
  };
}

async function resolveStartedState(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  teamId: string,
) {
  if (!teamId) {
    return "";
  }

  const cached = stateRef[teamId];
  if (cached) {
    return cached;
  }

  const result = await callLinear(api, cfg, "team(states)", {
    query: TEAM_STARTED_QUERY,
    variables: { id: teamId },
  });

  if (!result.ok) {
    return "";
  }

  const team = readObject(result.data.team);
  const states = readObject(team?.states);
  const nodes = readArray(states?.nodes);

  const picked = nodes.reduce<{ id: string; pos: number } | null>(
    (best, node) => {
      const item = readObject(node);
      if (!item) {
        return best;
      }
      const id = readString(item.id) ?? "";
      const pos = readNumber(item.position) ?? Number.POSITIVE_INFINITY;
      if (!id) {
        return best;
      }
      if (!best) {
        return { id, pos };
      }
      if (pos < best.pos) {
        return { id, pos };
      }
      return best;
    },
    null,
  );

  if (!picked) {
    return "";
  }

  stateRef[teamId] = picked.id;
  return picked.id;
}

async function updateIssue(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  issueId: string,
  input: Record<string, unknown>,
  label: string,
) {
  if (!issueId) {
    return;
  }

  const result = await callLinear(api, cfg, label, {
    query: ISSUE_UPDATE_MUTATION,
    variables: { id: issueId, input },
  });

  if (!result.ok) {
    return;
  }

  const root = readObject(result.data.issueUpdate);
  if (root && root.success === true) {
    return;
  }

  api.logger.warn?.(`linear ${label} failed`);
}

async function resolveViewer(api: OpenClawPluginApi, cfg: LinearCfg) {
  if (viewerRef.value) {
    return viewerRef.value;
  }

  const result = await callLinear(api, cfg, "viewer", {
    query: VIEWER_QUERY,
    variables: {},
  });

  if (!result.ok) {
    return "";
  }

  const viewer = readObject(result.data.viewer);
  const id = readString(viewer?.id) ?? "";

  if (id) {
    viewerRef.value = id;
  }

  return id;
}

async function callLinear(
  api: OpenClawPluginApi,
  cfg: LinearCfg,
  label: string,
  body: { query: string; variables: Record<string, unknown> },
): Promise<LinearResult> {
  const token = cfg.linearApiKey;

  if (!token) {
    warnMissingApiKey(api);
    return { ok: false };
  }

  const res = await fetch(LINEAR_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) {
    api.logger.warn?.(`linear ${label} failed: fetch error`);
    return { ok: false };
  }

  if (!res.ok) {
    const detail = await res.text();
    api.logger.warn?.(`linear ${label} failed (${res.status}): ${detail}`);
    return { ok: false };
  }

  const json = await res.json().catch(() => null);
  const root = readObject(json);

  if (!root) {
    api.logger.warn?.(`linear ${label} invalid response`);
    return { ok: false };
  }

  const errors = root.errors;
  if (Array.isArray(errors) && errors.length > 0) {
    const detail = errors
      .map((item) => readString(readObject(item)?.message) ?? "error")
      .filter((item) => Boolean(item))
      .join("; ");
    api.logger.warn?.(`linear ${label} failed: ${detail}`);
    return { ok: false };
  }

  const data = readObject(root.data);
  if (!data) {
    api.logger.warn?.(`linear ${label} missing data`);
    return { ok: false };
  }

  return { ok: true, data };
}

function warnMissingApiKey(api: OpenClawPluginApi) {
  if (warnRef.value) {
    return;
  }
  warnRef.value = true;
  api.logger.warn?.("linearApiKey missing; AgentActivity updates disabled");
}

async function loadCallGateway(api: OpenClawPluginApi): Promise<CallGateway> {
  if (callRef.value) {
    return callRef.value;
  }

  // Use callGateway from plugin API
  if (api.callGateway && typeof api.callGateway === "function") {
    callRef.value = api.callGateway as CallGateway;
    return api.callGateway as CallGateway;
  }

  throw new Error(
    "callGateway not available in plugin API. This plugin requires OpenClaw gateway.",
  );
}

// ============================================================================
// Configuration helpers
// ============================================================================

function normalizeCfg(input: Record<string, unknown> | undefined): LinearCfg {
  const cfg = input ?? {};
  return {
    devAgentId: readConfigString(cfg, "devAgentId"),
    linearWebhookSecret: readConfigString(cfg, "linearWebhookSecret"),
    linearApiKey: readConfigString(cfg, "linearApiKey"),
    notifyChannel: readConfigString(cfg, "notifyChannel"),
    notifyTo: readConfigString(cfg, "notifyTo"),
    notifyAccountId: readConfigString(cfg, "notifyAccountId"),
    repoByTeam: readConfigMap(cfg, "repoByTeam"),
    repoByProject: readConfigMap(cfg, "repoByProject"),
    defaultDir: readConfigString(cfg, "defaultDir"),
    delegateOnCreate: readConfigBool(cfg, "delegateOnCreate"),
    startOnCreate: readConfigBool(cfg, "startOnCreate"),
    externalUrlBase: readConfigString(cfg, "externalUrlBase"),
    externalUrlLabel: readConfigString(cfg, "externalUrlLabel"),
    streamActivities: readConfigBool(cfg, "streamActivities"),
    streamToolCalls: readConfigBool(cfg, "streamToolCalls"),
    streamIntervalMs: readConfigNumber(cfg, "streamIntervalMs"),
    streamMaxChars: readConfigNumber(cfg, "streamMaxChars"),
    streamToolAllowlist: readConfigStringArray(cfg, "streamToolAllowlist"),
    streamToolDenylist: readConfigStringArray(cfg, "streamToolDenylist"),
  };
}

function readConfigString(cfg: Record<string, unknown>, key: string) {
  const raw = cfg[key];
  if (typeof raw !== "string") {
    return undefined;
  }
  const value = raw.trim();
  return value || undefined;
}

function readConfigBool(cfg: Record<string, unknown>, key: string) {
  const raw = cfg[key];
  if (typeof raw !== "boolean") {
    return undefined;
  }
  return raw;
}

function readConfigNumber(cfg: Record<string, unknown>, key: string) {
  const raw = cfg[key];
  if (typeof raw !== "number" || Number.isNaN(raw)) {
    return undefined;
  }
  return raw;
}

function readConfigStringArray(cfg: Record<string, unknown>, key: string) {
  const raw = cfg[key];
  if (!Array.isArray(raw)) {
    return undefined;
  }
  const out = raw
    .map((item) => (typeof item === "string" ? item.trim() : ""))
    .filter((item) => Boolean(item));
  return out.length > 0 ? out : undefined;
}

function readConfigMap(cfg: Record<string, unknown>, key: string) {
  const raw = cfg[key];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return undefined;
  }
  const map = raw as Record<string, unknown>;
  const out = Object.entries(map).reduce<Record<string, string>>(
    (acc, [k, v]) => {
      if (typeof v === "string" && v.trim()) {
        acc[k] = v.trim();
      }
      return acc;
    },
    {},
  );
  return Object.keys(out).length > 0 ? out : undefined;
}

// ============================================================================
// Data extraction helpers
// ============================================================================

function resolveRepo(cfg: LinearCfg, team: string, proj: string) {
  if (proj && cfg.repoByProject?.[proj]) {
    return cfg.repoByProject[proj];
  }
  if (team && cfg.repoByTeam?.[team]) {
    return cfg.repoByTeam[team];
  }
  return cfg.defaultDir ?? "";
}

function resolveIssue(data: Record<string, unknown>) {
  const issue = readObject(data.issue);
  if (issue) {
    return issue;
  }
  const session = readObject(data.agentSession);
  return session ? readObject(session.issue) : undefined;
}

function resolveSessionId(data: Record<string, unknown>) {
  const session = readObject(data.agentSession);
  return readString(session?.id) ?? "";
}

function resolvePrompt(data: Record<string, unknown>) {
  const activity = readObject(data.agentActivity);
  const direct = readString(activity?.body);
  if (direct) {
    return direct;
  }
  const content = readObject(activity?.content);
  const body = readString(content?.body);
  if (body) {
    return body;
  }
  const comment = readObject(data.comment);
  return readString(comment?.body) ?? "";
}

function resolveSignal(data: Record<string, unknown>) {
  const activity = readObject(data.agentActivity);
  const signal = readString(activity?.signal);
  return signal || readString(data.signal) || "";
}

function resolveAction(data: Record<string, unknown>) {
  const action = readString(data.action);
  if (action === "created" || action === "prompted") {
    return action;
  }
  if (!action && readObject(data.agentSession)) {
    return "created";
  }
  return "";
}

function resolveContext(data: Record<string, unknown>) {
  return readString(data.promptContext) ?? "";
}

function resolveKey(input: unknown) {
  const obj = readObject(input);
  if (!obj) {
    return "";
  }
  return readString(obj.key) ?? readString(obj.id) ?? readString(obj.name) ?? "";
}

// ============================================================================
// Message builders
// ============================================================================

function buildLabel(id: string, title: string) {
  const label = id && title
    ? `Linear ${id} ${title}`
    : id
      ? `Linear ${id}`
      : title
        ? `Linear ${title}`
        : "Linear issue";
  return label.slice(0, 64);
}

function buildMessage(params: {
  action: string;
  id: string;
  title: string;
  url: string;
  desc: string;
  guidance: string;
  prompt: string;
  repo: string;
  session: string;
  context: string;
}) {
  const issueLine =
    params.id || params.title
      ? `Linear issue: ${params.id} ${params.title}`.trim()
      : "";
  const actionLine = params.action ? `Linear action: ${params.action}` : "";
  const guidanceLine = params.context
    ? ""
    : params.guidance
      ? `Guidance:\n${params.guidance}`
      : "";
  const descLine = params.context
    ? ""
    : params.desc
      ? `Description:\n${params.desc}`
      : "";
  const contextLine = params.context
    ? `Prompt context:\n${params.context}`
    : "";

  const lines = [
    actionLine,
    issueLine,
    params.url ? `URL: ${params.url}` : "",
    params.repo ? `Repo: ${params.repo}` : "",
    params.session ? `Agent session: ${params.session}` : "",
    contextLine,
    params.prompt ? `User prompt:\n${params.prompt}` : "",
    guidanceLine,
    descLine,
  ];

  return lines.filter(Boolean).join("\n\n");
}

function buildThought(action: string, id: string, title: string) {
  const target = id || title ? `${id} ${title}`.trim() : "Linear issue";
  if (action === "prompted") {
    return `Received an update on ${target}. Continuing work.`;
  }
  return `Starting work on ${target}.`;
}

function buildStopText(id: string, title: string) {
  const target = id || title ? `${id} ${title}`.trim() : "this request";
  return `Stop request received. I will halt work on ${target}.`;
}

function resolveExternal(cfg: LinearCfg, session: string, issueId: string) {
  const base = cfg.externalUrlBase ?? "";
  const label = cfg.externalUrlLabel ?? "OpenClaw session";
  const url = buildExternalUrl(base, session, issueId);
  return url ? { url, label } : null;
}

function buildExternalUrl(base: string, session: string, issueId: string) {
  const raw = base.trim();
  if (!raw) {
    return "";
  }

  const sessionToken = session ?? "";
  const issueToken = issueId ?? "";
  const needsSession = raw.includes("{session}") || raw.includes("${session}");
  const needsIssue = raw.includes("{issue}") || raw.includes("${issue}");

  if (needsSession && !sessionToken) {
    return "";
  }
  if (needsIssue && !issueToken) {
    return "";
  }

  if (needsSession || needsIssue) {
    return raw
      .replaceAll("{session}", sessionToken)
      .replaceAll("${session}", sessionToken)
      .replaceAll("{issue}", issueToken)
      .replaceAll("${issue}", issueToken);
  }

  if (!URL.canParse(raw)) {
    return "";
  }

  const url = new URL(raw);
  if (sessionToken) {
    url.searchParams.set("session", sessionToken);
  }
  if (issueToken) {
    url.searchParams.set("issue", issueToken);
  }
  return url.toString();
}

function resolveFlag(value: boolean | undefined, fallback: boolean) {
  return typeof value === "boolean" ? value : fallback;
}

// ============================================================================
// Response extraction
// ============================================================================

function buildAgentResponse(input: unknown) {
  const payload = readObject(input);
  if (!payload) {
    return "";
  }

  const result = readObject(payload.result);
  const payloads = readArray(result?.payloads);
  const text = collectPayloadText(payloads);

  if (text) {
    return text;
  }

  const status = readString(payload.status) ?? "";
  if (status === "ok") {
    return "Agent completed with no reply.";
  }

  return "";
}

function collectPayloadText(payloads: unknown[]) {
  if (payloads.length === 0) {
    return "";
  }

  const lines: string[] = [];
  const seenMedia = new Set<string>();

  for (const entry of payloads) {
    const item = readObject(entry);
    if (!item) {
      continue;
    }

    const text = readString(item.text);
    if (text) {
      lines.push(text);
    }

    const media = collectMediaUrls(item, seenMedia);
    for (const url of media) {
      lines.push(`Media: ${url}`);
    }
  }

  return lines.join("\n\n");
}

function collectMediaUrls(item: Record<string, unknown>, seen: Set<string>) {
  const urls: string[] = [];

  const direct = readString(item.mediaUrl);
  if (direct && !seen.has(direct)) {
    seen.add(direct);
    urls.push(direct);
  }

  const list = readArray(item.mediaUrls);
  for (const entry of list) {
    const url = readString(entry);
    if (url && !seen.has(url)) {
      seen.add(url);
      urls.push(url);
    }
  }

  return urls;
}

function normalizeKey(input: string) {
  const lower = input.trim().toLowerCase();
  if (!lower) {
    return "issue";
  }
  return (
    lower
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+/, "")
      .replace(/-+$/, "")
      .slice(0, 64) || "issue"
  );
}

// ============================================================================
// Utility functions
// ============================================================================

function readString(input: unknown) {
  if (typeof input !== "string") {
    return undefined;
  }
  const value = input.trim();
  return value || undefined;
}

function readNumber(input: unknown) {
  if (typeof input !== "number" || Number.isNaN(input)) {
    return undefined;
  }
  return input;
}

function readObject(input: unknown) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return undefined;
  }
  return input as Record<string, unknown>;
}

function readArray(input: unknown) {
  return Array.isArray(input) ? input : [];
}

function logEvent(
  api: OpenClawPluginApi,
  label: string,
  data: Record<string, unknown>,
) {
  const action = readString(data.action) ?? "";
  const name = action ? `${label} ${action}` : label;
  api.logger.info?.(`linear ${name}`);
}

function verifySignature(
  secret: string,
  signature: string | undefined,
  raw: Buffer,
) {
  if (!signature) {
    return false;
  }

  const header = Buffer.from(signature, "hex");
  const digest = createHmac("sha256", secret).update(raw).digest();

  if (header.length !== digest.length) {
    return false;
  }

  return timingSafeEqual(digest, header);
}

function readHeader(req: IncomingMessage, name: string) {
  const value = req.headers[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return typeof value === "string" ? value : undefined;
}

function readBody(req: IncomingMessage, limit: number) {
  return new Promise<
    { ok: true; body: Buffer } | { ok: false; status: number; error: string }
  >((resolve) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let done = false;

    const finish = (
      value:
        | { ok: true; body: Buffer }
        | { ok: false; status: number; error: string },
    ) => {
      if (done) return;
      done = true;
      resolve(value);
    };

    req.on("data", (chunk) => {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      size += buf.length;

      if (size > limit) {
        req.destroy();
        finish({ ok: false, status: 413, error: "payload too large" });
        return;
      }

      chunks.push(buf);
    });

    req.on("end", () => {
      finish({ ok: true, body: Buffer.concat(chunks) });
    });

    req.on("error", () => {
      finish({ ok: false, status: 400, error: "read error" });
    });
  });
}
