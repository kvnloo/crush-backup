package tools

import (
	"context"
	_ "embed"
	"fmt"
	"html/template"
	"strings"
	"time"

	"charm.land/fantasy"
	"github.com/charmbracelet/crush/internal/pubsub"
	"github.com/charmbracelet/crush/internal/shell"
)

const (
	JobOutputToolName = "job_output"
)

//go:embed job_output.md.tpl
var jobOutputDescriptionTmpl string

var jobOutputDescriptionTpl = template.Must(template.New("jobOutputDescription").Parse(jobOutputDescriptionTmpl))

type JobOutputParams struct {
	ShellID string `json:"shell_id" description:"The ID of the background shell to retrieve output from"`
	Wait    bool   `json:"wait" description:"If true, block until the background shell completes before returning output"`
}

type JobOutputResponseMetadata struct {
	ShellID          string `json:"shell_id"`
	Command          string `json:"command"`
	Description      string `json:"description"`
	Done             bool   `json:"done"`
	WorkingDirectory string `json:"working_directory"`
}

func NewJobOutputTool(spillDir string, jobs BackgroundJobs, progress pubsub.Publisher[ToolProgress]) fantasy.AgentTool {
	return fantasy.NewAgentTool(
		JobOutputToolName,
		jobOutputToolDescription(jobs.Reports()),
		func(ctx context.Context, params JobOutputParams, call fantasy.ToolCall) (fantasy.ToolResponse, error) {
			if params.ShellID == "" {
				return fantasy.NewTextErrorResponse("missing shell_id"), nil
			}

			bgManager := shell.GetBackgroundShellManager()
			scope, err := jobs.Scope(ctx)
			if err != nil {
				return fantasy.NewTextErrorResponse(err.Error()), nil
			}
			bgShell, ok := bgManager.Lookup(scope, params.ShellID)
			if !ok {
				return fantasy.NewTextErrorResponse(fmt.Sprintf("background shell not found: %s", params.ShellID)), nil
			}

			// Reading the result here stands in for the completion report,
			// so hold it back and drop it if this call sees the finish.
			endRead := bgShell.Read()
			if params.Wait {
				waitWithProgress(ctx, bgShell, newProgressReporter(progress, GetSessionFromContext(ctx), call.ID))
			}

			result := ReadJobResult(bgShell, spillDir)
			// A cancelled call's result may never reach the model, so it
			// does not stand in for the report.
			endRead(result.Done && ctx.Err() == nil)

			status := "running"
			if result.Done {
				status = "completed"
			}

			metadata := JobOutputResponseMetadata{
				ShellID:          params.ShellID,
				Command:          bgShell.Command,
				Description:      bgShell.Description,
				Done:             result.Done,
				WorkingDirectory: bgShell.WorkingDir,
			}

			text := fmt.Sprintf("Status: %s\n\n%s", status, result.Text)
			return fantasy.WithResponseMetadata(fantasy.NewTextResponse(text), metadata), nil
		},
	)
}

// waitWithProgress blocks until the shell finishes or ctx is cancelled,
// publishing live output while it waits.
func waitWithProgress(ctx context.Context, bgShell *shell.BackgroundShell, progress *progressReporter) {
	select {
	case <-ctx.Done():
		return
	case <-bgShell.Done():
		return
	default:
	}
	progress.report(bgShell)

	ticker := time.NewTicker(progressPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-bgShell.Done():
			return
		case <-ticker.C:
			progress.report(bgShell)
		}
	}
}

// jobOutputToolDescription is the tool's description, which points the
// agent at job reports when they are on.
func jobOutputToolDescription(jobReports bool) string {
	var out strings.Builder
	if err := jobOutputDescriptionTpl.Execute(&out, struct{ JobReports bool }{jobReports}); err != nil {
		// The template is embedded, so this only fails on a broken build.
		panic("failed to execute job_output description template: " + err.Error())
	}
	return out.String()
}
