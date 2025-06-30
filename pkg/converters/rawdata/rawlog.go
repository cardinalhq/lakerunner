package rawdata

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

// Process converts a map[string]interface{} to a LogData structure.
// It extracts the timestamp and message from the specified fields,
// and ignores fields specified in IgnoreFields.
func (p *RawLogProcessor) Process(data map[string]any) (plog.LogResources, error) {
}
