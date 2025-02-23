import { ENV_VARS } from "../env";
import { NodeServiceClient } from "@selfage/node_service_client";

export let SERVICE_CLIENT = NodeServiceClient.create(ENV_VARS.internalOrigin);
