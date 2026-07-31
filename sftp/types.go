package sftp

// ResponseList is the list response.
type ResponseList[T any] struct {
	Items []T `json:"items"`
}
