{{/* Base name + release-qualified fullname (standard Helm idiom). */}}
{{- define "reverse-rusty.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "reverse-rusty.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "reverse-rusty.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "reverse-rusty.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "reverse-rusty.selectorLabels" -}}
app.kubernetes.io/name: {{ include "reverse-rusty.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Fully-qualified image ref; tag defaults to the chart appVersion. */}}
{{- define "reverse-rusty.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/* Per-component object names. */}}
{{- define "reverse-rusty.shardName" -}}{{ include "reverse-rusty.fullname" . }}-shard{{- end -}}
{{- define "reverse-rusty.controlName" -}}{{ include "reverse-rusty.fullname" . }}-control{{- end -}}
{{- define "reverse-rusty.coordinatorName" -}}{{ include "reverse-rusty.fullname" . }}-coordinator{{- end -}}

{{/* http vs https for mesh URLs — must match the served transport (ADR-082). */}}
{{- define "reverse-rusty.scheme" -}}
{{- if .Values.tls.enabled -}}https{{- else -}}http{{- end -}}
{{- end -}}

{{/* Stable DNS FQDN for shard ordinal $i (headless service per StatefulSet). */}}
{{- define "reverse-rusty.shardFqdn" -}}
{{- $i := .index -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "reverse-rusty.shardName" .root) $i (include "reverse-rusty.shardName" .root) .root.Release.Namespace .root.Values.clusterDomain -}}
{{- end -}}

{{/* Stable DNS FQDN for control ordinal $i. */}}
{{- define "reverse-rusty.controlFqdn" -}}
{{- $i := .index -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "reverse-rusty.controlName" .root) $i (include "reverse-rusty.controlName" .root) .root.Release.Namespace .root.Values.clusterDomain -}}
{{- end -}}
