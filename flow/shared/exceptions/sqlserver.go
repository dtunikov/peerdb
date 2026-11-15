package exceptions

import "fmt"

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
