package rawdata

import "go.opentelemetry.io/collector/pdata/plog"

var (
	defaultTimestampFields = []string{"timestamp", "time", "ts"}
	defaultMessageFields   = []string{"message", "msg"}
	defaultIgnoreFields    = []string{}
)

type RawLogProcessor struct {
	TimestampFields []string
	MessageFields   []string
	IgnoreFields    []string
}

func NewRawLogProcessor(timestampFields []string, messageFields []string, ignoreFields []string) *RawLogProcessor {
	return &RawLogProcessor{
		TimestampFields: timestampFields,
		MessageFields:   messageFields,
		IgnoreFields:    ignoreFields,
	}
}

// add bucket.name, file.name, and file.type to the resources we write out

// Process converts a map[string]interface{} to a LogData structure.
// It extracts the timestamp and message from the specified fields,
// and ignores fields specified in IgnoreFields.
func (p *RawLogProcessor) Process(data map[string]any) (plog.Logs, error) {
}
