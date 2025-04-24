package file

// ResponseListKeys is the response from the Redis list SCAN command.
type ResponseListKeys struct {
	Keys []string `json:"keys"`
}

// CreateAny is a struct for the `create.Create` `Any` field.
type CreateAny struct {
	// Create directory and file if it does not exist.
	CreateIfNotExist bool `json:"create_if_not_exist" default:"true"`
}
