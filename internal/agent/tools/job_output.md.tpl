{{- if .JobReports -}}
Get stdout/stderr from a background shell by ID; returns whatever the job has printed so far. A finished job reports back by itself with a `<job-report>`, so use this to check on a job that is still running, such as a server's startup log, not to collect its final result. Set wait=true only when blocking until the job finishes is what you want; ending your turn and letting the report wake you is usually better.
{{- else -}}
Get stdout/stderr from a background shell by ID; returns whatever the job has printed so far. Call it when you need the job's result, not right after starting the job, and set wait=true only when blocking until the job finishes is what you want.
{{- end }}
