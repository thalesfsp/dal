//////
// Shared utils.
//////

package shared

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/thalesfsp/customerror"
)

// GenerateUUID generates a RFC4122 UUID and DCE 1.1: Authentication and
// Security Services.
func GenerateUUID() string {
	return uuid.New().String()
}

// GenerateID generates MD5 hash (content-based) based on content.
// Good to be used to avoid duplicated messages.
func GenerateID(ct string) string {
	// Convert the string to bytes
	data := []byte(strings.Trim(ct, "\f\t\r\n "))

	// Create a new SHA-256 hash
	h := sha256.New()

	// Write the bytes to the hash
	h.Write(data)

	// Get the hash sum as a byte slice
	hashSum := h.Sum(nil)

	// Convert the hash sum to a hex string
	hashString := hex.EncodeToString(hashSum)

	return hashString
}

// SliceContains returns true if the slice contains the string.
//
// NOTE: It's case insensitive.
//
// NOTE: @andres moved to here.
func SliceContains(source []string, text string) bool {
	for _, s := range source {
		if strings.EqualFold(s, text) {
			return true
		}
	}

	return false
}

// Unmarshal with custom error.
func Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, &v); err != nil {
		return customerror.NewFailedToError("to unmarshal",
			customerror.WithError(err),
		)
	}

	return nil
}

// Marshal with custom error.
func Marshal(v any) ([]byte, error) {
	data, err := json.Marshal(&v)
	if err != nil {
		return nil, customerror.NewFailedToError("to marshal",
			customerror.WithError(err),
		)
	}

	return data, nil
}

// Decode process stream `r` into `v` and returns an error if any.
func Decode(r io.Reader, v any) error {
	if err := json.NewDecoder(r).Decode(v); err != nil {
		return customerror.NewFailedToError("decode",
			customerror.WithError(err),
		)
	}

	return nil
}

// Encode process `v` into stream `w` and returns an error if any.
func Encode(w io.Writer, v any) error {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return customerror.NewFailedToError("encode",
			customerror.WithError(err),
		)
	}

	return nil
}

// ReadAll reads all the data from `r` and returns an error if any.
func ReadAll(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, customerror.NewFailedToError("read response body", customerror.WithError(err))
	}

	return b, nil
}

// PrintErrorMessages prints the concatenated error messages.
func PrintErrorMessages(errors ...error) string {
	finalErrMsg := ""

	for _, err := range errors {
		finalErrMsg += err.Error() + ". "
	}

	// Trim the last dot.
	finalErrMsg = strings.TrimSuffix(finalErrMsg, ". ")

	return finalErrMsg
}

// TargetName returns the provided target name, or the configured one. A target,
// depending on the storage, is a collection, a table, a bucket, etc.
// For ElasticSearch - as it doesn't have a concept of a database - the target
// is the index.
func TargetName(name, alternative string) (string, error) {
	if name != "" {
		return name, nil
	}

	if alternative != "" {
		return alternative, nil
	}

	return "", customerror.NewMissingError("target name")
}
