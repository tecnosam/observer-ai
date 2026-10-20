{{/* Names, labels, and the engine's config. */}}

{{- define "observer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "observer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "observer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "observer.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "observer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "observer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: engine
{{- end }}

{{- define "observer.gatewaySelectorLabels" -}}
app.kubernetes.io/name: {{ include "observer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: trace-gateway
{{- end }}

{{- define "observer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "observer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "observer.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/* The bundled NATS Service, named as the NATS chart names it. */}}
{{- define "observer.natsName" -}}
{{- if .Values.nats.fullnameOverride }}
{{- .Values.nats.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else if contains "nats" .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-nats" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/* Where the queues live: the bundled NATS, queue.url, or "" for memory. */}}
{{- define "observer.queueURL" -}}
{{- if .Values.nats.enabled }}
{{- printf "nats://%s.%s.svc:4222" (include "observer.natsName" .) .Release.Namespace }}
{{- else }}
{{- .Values.queue.url }}
{{- end }}
{{- end }}

{{- define "observer.inMemory" -}}
{{- if eq (include "observer.queueURL" .) "" }}true{{ end }}
{{- end }}

{{/* ClientIP when traces must stick to a pod and no gateway routes them. */}}
{{- define "observer.sessionAffinity" -}}
{{- if .Values.service.sessionAffinity }}
{{- .Values.service.sessionAffinity }}
{{- else if and (include "observer.inMemory" .) (not .Values.traceRouting.gateway.enabled) }}
{{- "ClientIP" }}
{{- else }}
{{- "None" }}
{{- end }}
{{- end }}

{{/*
observer.yaml: the user's config, with the listen addresses the container
exposes and the queue URL the chart knows. With the bundled NATS, streams
default to one copy per NATS server.
*/}}
{{- define "observer.config" -}}
{{- $cfg := deepCopy .Values.config }}
{{- $server := get $cfg "server" | default dict }}
{{- $_ := set $server "addr" ":8080" }}
{{- $_ := set $cfg "server" $server }}
{{- $otlp := get $cfg "otlp" | default dict }}
{{- $_ := set $otlp "grpc_addr" ":4317" }}
{{- $_ := set $otlp "http_addr" ":4318" }}
{{- $_ := set $cfg "otlp" $otlp }}
{{- $queue := get $cfg "queue" | default dict }}
{{- $url := include "observer.queueURL" . }}
{{- if $url }}
{{- $_ := set $queue "url" $url }}
{{- end }}
{{- if and .Values.nats.enabled (not (hasKey $queue "replicas")) }}
{{- $_ := set $queue "replicas" (int .Values.nats.config.cluster.replicas) }}
{{- end }}
{{- $_ := set $cfg "queue" $queue }}
{{- toYaml $cfg }}
{{- end }}

{{- define "observer.manifestsConfigMap" -}}
{{- if .Values.manifests.existingConfigMap }}
{{- .Values.manifests.existingConfigMap }}
{{- else }}
{{- printf "%s-manifests" (include "observer.fullname" .) }}
{{- end }}
{{- end }}
