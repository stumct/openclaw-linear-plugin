import {
  createLinearWebhook,
  registerLinearHooks,
  OpenClawPluginApi,
} from "./src/linear-webhook.js";

export type { OpenClawPluginApi };

export default function register(api: OpenClawPluginApi) {
  registerLinearHooks(api);
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
