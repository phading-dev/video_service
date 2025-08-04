import { ENV_VARS } from "./env_vars";
import { VIDEO_NODE_SERVICE } from "@phading/video_service_interface/service";
import { writeFileSync } from "fs";

export function generate(env: string) {
  let corsTemplate = `[
  {
    "maxAgeSeconds": 3600,
    "method": [
      "HEAD",
      "PUT",
      "OPTIONS"
    ],
    "origin": [
      "${ENV_VARS.externalOrigin}"
    ],
    "responseHeader": [
      "Content-Type",
      "Content-Length"
    ]
  }
]
`;
  writeFileSync(`${env}/cors.json`, corsTemplate);

  let turnupTemplate = `#!/bin/bash
# GCP auth
gcloud auth application-default login
gcloud config set project ${ENV_VARS.projectId}

# Create service account
gcloud iam service-accounts create ${ENV_VARS.builderAccount}

# Grant permissions to the service account
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member="serviceAccount:${ENV_VARS.builderAccount}@${ENV_VARS.projectId}.iam.gserviceaccount.com" --role='roles/cloudbuild.builds.builder' --condition=None
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member="serviceAccount:${ENV_VARS.builderAccount}@${ENV_VARS.projectId}.iam.gserviceaccount.com" --role='roles/container.developer' --condition=None
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member="serviceAccount:${ENV_VARS.builderAccount}@${ENV_VARS.projectId}.iam.gserviceaccount.com" --role='roles/spanner.databaseAdmin' --condition=None

# Set k8s cluster
gcloud container clusters get-credentials ${ENV_VARS.clusterName} --location=${ENV_VARS.clusterRegion}

# Create the service account
kubectl create serviceaccount ${ENV_VARS.serviceAccount} --namespace default

# Grant database permissions to the service account
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member=principal://iam.googleapis.com/projects/${ENV_VARS.projectNumber}/locations/global/workloadIdentityPools/${ENV_VARS.projectId}.svc.id.goog/subject/ns/default/sa/${ENV_VARS.serviceAccount} --role=roles/spanner.databaseUser --condition=None
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member=principal://iam.googleapis.com/projects/${ENV_VARS.projectNumber}/locations/global/workloadIdentityPools/${ENV_VARS.projectId}.svc.id.goog/subject/ns/default/sa/${ENV_VARS.serviceAccount} --role=roles/storage.objectUser --condition=None

# Create Spanner database
gcloud spanner databases create ${ENV_VARS.spannerDatabaseId} --instance=${ENV_VARS.spannerInstanceId}

# Set up GCS bucket for video storage
gcloud storage buckets update gs://${ENV_VARS.gcsVideoBucketName} --cors-file=./${env}/cors.json
`;
  writeFileSync(`${env}/turnup.sh`, turnupTemplate);

  let cloudbuildTemplate = `steps:
- name: 'node:20.12.1'
  entrypoint: 'npm'
  args: ['ci']
- name: 'node:20.12.1'
  entrypoint: 'npx'
  args: ['spanage', 'update', 'db/ddl', '-p', '${ENV_VARS.projectId}', '-i', '${ENV_VARS.spannerInstanceId}', '-d', '${ENV_VARS.spannerDatabaseId}']
- name: node:20.12.1
  entrypoint: npx
  args: ['bundage', 'bfn', '${env}/main', 'main_bin', '-t', 'bin']
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', 'gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest', '-f', '${env}/Dockerfile', '.']
- name: "gcr.io/cloud-builders/docker"
  args: ['push', 'gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest']
- name: 'gcr.io/cloud-builders/kubectl'
  args: ['apply', '-f', '${env}/service.yaml']
  env:
    - 'CLOUDSDK_CONTAINER_CLUSTER=${ENV_VARS.clusterName}'
    - 'CLOUDSDK_COMPUTE_REGION=${ENV_VARS.clusterRegion}'
- name: 'gcr.io/cloud-builders/kubectl'
  args: ['rollout', 'restart', 'deployment', '${ENV_VARS.releaseServiceName}-deployment']
  env:
    - 'CLOUDSDK_CONTAINER_CLUSTER=${ENV_VARS.clusterName}'
    - 'CLOUDSDK_COMPUTE_REGION=${ENV_VARS.clusterRegion}'
options:
  logging: CLOUD_LOGGING_ONLY
`;
  writeFileSync(`${env}/cloudbuild.yaml`, cloudbuildTemplate);

  let dockerTemplate = `FROM node:20.12.1

RUN apt-get update && apt-get install -y ffmpeg unzip curl && \\
    curl https://rclone.org/install.sh | bash && \\
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json .
COPY package-lock.json .
COPY bin/ .
RUN npm ci --omit=dev

EXPOSE ${ENV_VARS.port}
CMD ["node", "main_bin"]
`;
  writeFileSync(`${env}/Dockerfile`, dockerTemplate);

  let serviceTemplate = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${ENV_VARS.releaseServiceName}-deployment
spec:
  replicas: ${ENV_VARS.replicas}
  selector:
    matchLabels:
      app: ${ENV_VARS.releaseServiceName}-pod
  template:
    metadata:
      labels:
        app: ${ENV_VARS.releaseServiceName}-pod
      annotations:
        gke-gcsfuse/volumes: 'true'
    spec:
      serviceAccountName: ${ENV_VARS.serviceAccount}
      containers:
      - name: ${ENV_VARS.releaseServiceName}-container
        image: gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest
        ports:
        - containerPort: ${ENV_VARS.port}
        livenessProbe:
          httpGet:
            path: /healthz
            port: ${ENV_VARS.port}
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /readiness
            port: ${ENV_VARS.port}
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            cpu: "${ENV_VARS.cpu}"
            memory: "${ENV_VARS.memory}"
            ephemeral-storage: "${ENV_VARS.storage}"
          limits:
            cpu: "${ENV_VARS.cpuLimit}"
            memory: "${ENV_VARS.memoryLimit}"
            ephemeral-storage: "${ENV_VARS.storageLimit}"
        volumeMounts:
        - name: video-volume
          mountPath: ${ENV_VARS.gcsVideoMountedLocalDir}
      volumes:
      - name: video-volume
        csi:
          driver: gcsfuse.csi.storage.gke.io
          volumeAttributes:
            bucketName: ${ENV_VARS.gcsVideoBucketName}
            mountOptions: 'implicit-dirs'
---
apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: ${ENV_VARS.releaseServiceName}-monitoring
spec:
  selector:
    matchLabels:
      app: ${ENV_VARS.releaseServiceName}-pod
  endpoints:
  - port: ${ENV_VARS.port}
    path: /metricsz
    interval: 30s
---
apiVersion: v1
kind: Service
metadata:
  name: ${ENV_VARS.releaseServiceName}
spec:
  selector:
    app: ${ENV_VARS.releaseServiceName}-pod
  ports:
    - protocol: TCP
      port: ${ENV_VARS.port}
      targetPort: ${ENV_VARS.port}
  type: ClusterIP
---
apiVersion: networking.gke.io/v1
kind: HealthCheckPolicy
metadata:
  name: ${ENV_VARS.releaseServiceName}-lb-health-check
spec:
  default:
    config:
      type: HTTP
      httpHealthCheck:
        port: ${ENV_VARS.port}
        requestPath: /healthz
  targetRef:
    group: ""
    kind: Service
    name: ${ENV_VARS.releaseServiceName}
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: ${ENV_VARS.releaseServiceName}-route-internal
spec:
  parentRefs:
  - name: ${ENV_VARS.internalGatewayName}
    sectionName: http
  rules:
  - matches:
    - path:
        type: PathPrefix
        value: ${VIDEO_NODE_SERVICE.path}
    backendRefs:
    - name: ${ENV_VARS.releaseServiceName}
      port: ${ENV_VARS.port}
`;
  writeFileSync(`${env}/service.yaml`, serviceTemplate);

  let mainTemplate = `import "./env";
import "../main";
`;
  writeFileSync(`${env}/main.ts`, mainTemplate);
}

import "./dev/env";
generate("dev");

import "./prod/env";
generate("prod");
