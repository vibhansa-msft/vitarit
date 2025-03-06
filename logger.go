package vitarit

const (
	LOG_DEBUG = iota
	LOG_INFO
	LOG_WARNING
	LOG_ERROR
	LOG_CRITICAL
)

var logFunc func(int, string)

func logMessage(level int, message string) {
	if logFunc != nil {
		logFunc(level, message)
	}
}
