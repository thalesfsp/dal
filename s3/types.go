package s3

// ResponseListKeys is the response from the Redis list SCAN command.
type ResponseListKeys struct {
	Keys []string `json:"keys"`
}
