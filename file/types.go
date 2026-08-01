package file

// ResponseList is the list response.
type ResponseList[T any] struct {
	Items []T `json:"items"`
}

// CreateAny is a struct for the `create.Create` `Any` field.
type CreateAny struct {
	// Create directory and file if it does not exist.
	CreateIfNotExist bool `default:"true" json:"create_if_not_exist"`
}
