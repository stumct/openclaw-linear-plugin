# OpenClaw Linear Plugin

Receive [Linear](https://linear.app) Agent Session webhooks and run [OpenClaw](https://github.com/openclaw/openclaw) agents on issues.

When a Linear issue is delegated to your agent app, this plugin:
1. Receives the webhook
2. Starts an OpenClaw agent session
3. Posts the agent's response back to Linear

## Installation

```bash
npm install openclaw-linear-plugin
```

## Prerequisites

### 1. Create a Linear Agent Application

1. Go to [Linear Settings → API → Applications](https://linear.app/settings/api/applications/new)
2. Create a new application with:
   - Name: Your agent name (e.g., "DevBot")
   - Webhook URL: `https://your-gateway/plugins/openclaw-linear-plugin/linear`
   - Enable **Agent session events** in webhook categories

### 2. Install the App via OAuth

Complete the OAuth flow with these parameters:
```
https://linear.app/oauth/authorize?
  client_id=YOUR_CLIENT_ID&
  response_type=code&
  scope=read,write,app:assignable,app:mentionable&
  actor=app&
  redirect_uri=YOUR_REDIRECT_URI
```

The `actor=app` parameter is required for agent functionality.

### 3. Expose Your Gateway

The webhook URL must be publicly accessible. If using [Tailscale](https://tailscale.com), enable Funnel:

```bash
tailscale funnel 18789  # Replace with your gateway port
```

## Configuration

Add to your `openclaw.json`:

```json
{
  "plugins": {
    "entries": {
      "openclaw-linear-plugin": {
        "enabled": true,
        "config": {
          "devAgentId": "dev",
          "linearWebhookSecret": "lin_wh_...",
          "linearApiKey": "lin_oauth_...",
          "defaultDir": "/path/to/repo",
          "delegateOnCreate": true,
          "startOnCreate": true
        }
      }
    }
  }
}
```

### Config Options

| Option | Type | Description |
|--------|------|-------------|
| `devAgentId` | string | Agent ID to handle issues (default: `"dev"`) |
| `linearWebhookSecret` | string | Webhook signing secret from Linear |
| `linearApiKey` | string | OAuth token from Linear app installation |
| `defaultDir` | string | Default repository directory |
| `repoByTeam` | object | Map team keys to repo directories |
| `repoByProject` | object | Map project keys to repo directories |
| `delegateOnCreate` | boolean | Auto-delegate issues to app (default: `true`) |
| `startOnCreate` | boolean | Move issues to "started" state (default: `true`) |
| `notifyChannel` | string | Optional: Channel for notifications (e.g., `"discord"`) |
| `notifyTo` | string | Optional: Notification target (e.g., `"channel:123456"`) |
| `externalUrlBase` | string | Optional: External session URL template |
| `externalUrlLabel` | string | Optional: Label for external URL link |
| `streamActivities` | boolean | Post progress activities during agent runs (default: `false`) |
| `streamToolCalls` | boolean | Post tool call activities during agent runs (default: `true`) |
| `streamIntervalMs` | number | Heartbeat interval for progress thoughts in ms (default: `120000`, `0` disables) |
| `streamMaxChars` | number | Max chars for streamed tool args/results (default: `500`) |
| `streamToolAllowlist` | string[] | Tool name allowlist for streaming (supports `*` wildcards) |
| `streamToolDenylist` | string[] | Tool name denylist for streaming (supports `*` wildcards) |

### External URL Template

Use placeholders for dynamic URLs:
```json
{
  "externalUrlBase": "https://your-app.com/sessions/{session}",
  "externalUrlLabel": "View Session"
}
```

Supports `{session}` and `{issue}` placeholders.

### Streaming Progress (Optional)

Enable streaming to post tool activity updates and periodic heartbeat thoughts:

```json
{
  "streamActivities": true,
  "streamToolCalls": true,
  "streamIntervalMs": 120000,
  "streamMaxChars": 500,
  "streamToolAllowlist": ["*"],
  "streamToolDenylist": ["browser", "canvas"]
}
```

## How It Works

1. **Webhook Reception**: Linear sends an AgentSessionEvent when an issue is delegated to your app
2. **Initial Response**: Plugin posts a "thinking" activity within 5 seconds
3. **Agent Execution**: OpenClaw agent runs with the issue context
4. **Response Posting**: Agent's reply is posted back to Linear as a response activity

### Webhook Endpoint

```
POST /plugins/openclaw-linear-plugin/linear
```

Legacy path (still supported):

```
POST /plugins/linear/linear
```

The plugin verifies the `Linear-Signature` header when `linearWebhookSecret` is configured.

## Troubleshooting

### "Did not respond" in Linear

- Check that your webhook URL is publicly accessible
- Verify `linearApiKey` has correct OAuth scopes
- Check OpenClaw logs for errors

### Webhook signature failures

- Ensure `linearWebhookSecret` matches Linear's signing secret
- Check for clock skew (webhooks older than 60s are rejected)

### Agent not running

- Verify `devAgentId` matches an agent in your OpenClaw config
- Check that the agent has necessary tool permissions

## Example Webhook Payload

```json
{
  "action": "created",
  "agentSession": {
    "id": "session_abc123",
    "issue": {
      "id": "issue_xyz789",
      "identifier": "ENG-123",
      "title": "Fix login bug",
      "url": "https://linear.app/team/issue/ENG-123"
    }
  },
  "promptContext": "<issue>...</issue>",
  "guidance": "Follow coding standards"
}
```

## License

MIT © stumct
