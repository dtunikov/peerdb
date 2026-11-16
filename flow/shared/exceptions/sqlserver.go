package exceptions

import (
	"fmt"
	"time"
)

// SqlServerCDCDataCleanedUpError means the CDC cleanup job removed changes the mirror has not read yet.
// Only a resync can recover.
type SqlServerCDCDataCleanedUpError struct {
	CaptureInstance string
	MinLSN          string
	FromLSN         string
}

func NewSqlServerCDCDataCleanedUpError(captureInstance string, minLSN string, fromLSN string) *SqlServerCDCDataCleanedUpError {
	return &SqlServerCDCDataCleanedUpError{CaptureInstance: captureInstance, MinLSN: minLSN, FromLSN: fromLSN}
}

func (e *SqlServerCDCDataCleanedUpError) Error() string {
	return fmt.Sprintf(
		"changes of capture instance %s were cleaned up before they were read (min LSN %s, mirror at %s), resync the mirror",
		e.CaptureInstance, e.MinLSN, e.FromLSN)
}

// SqlServerCaptureJobStalledError means the CDC capture job stopped scanning the log, usually because
// SQL Server Agent or the capture job is stopped. The mirror resumes once the job runs again.
type SqlServerCaptureJobStalledError struct {
	// zero when no scan was recorded since the instance started
	SinceLastScan time.Duration
}

func NewSqlServerCaptureJobStalledError(sinceLastScan time.Duration) *SqlServerCaptureJobStalledError {
	return &SqlServerCaptureJobStalledError{SinceLastScan: sinceLastScan}
}

func (e *SqlServerCaptureJobStalledError) Error() string {
	const hint = "check that SQL Server Agent is running and start the capture job with sys.sp_cdc_start_job"
	if e.SinceLastScan == 0 {
		return "CDC capture job has not scanned the log since the instance started, " + hint
	}
	return fmt.Sprintf("CDC capture job has not scanned the log for %s, %s", e.SinceLastScan.Round(time.Second), hint)
}
