import { createLinearWebhook, OpenClawPluginApi } from "./src/linear-webhook.js";

export type { OpenClawPluginApi };

export default function register(api: OpenClawPluginApi) {
  const handler = createLinearWebhook(api);
  api.registerHttpRoute({
    path: "/plugins/openclaw-linear-plugin/linear",
    handler,
  });
  api.registerHttpRoute({
    path: "/plugins/linear/linear",
    handler,
  });
}
