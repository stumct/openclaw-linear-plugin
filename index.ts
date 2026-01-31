import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

import { createLinearWebhook } from "./src/linear-webhook.js";

export default function register(api: OpenClawPluginApi) {
  api.registerHttpRoute({
    path: "/plugins/linear/linear",
    handler: createLinearWebhook(api),
  });
}
