import { createLinearWebhook, OpenClawPluginApi } from "./src/linear-webhook.js";

export type { OpenClawPluginApi };

export default function register(api: OpenClawPluginApi) {
  api.registerHttpRoute({
    path: "/plugins/linear/linear",
    handler: createLinearWebhook(api),
  });
}
