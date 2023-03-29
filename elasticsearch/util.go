package elasticsearch

import (
	"io"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/shared"
)

// Parse ES response body.
func parseResponseBody(r io.Reader, v any) error {
	if err := shared.Decode(r, v); err != nil {
		return err
	}

	return nil
}

// Parse ES response body when it's an error.
func parseResponseBodyError(r io.Reader) (*ResponseError, error) {
	responseError := &ResponseErrorFromES{}
	if err := parseResponseBody(r, responseError); err != nil {
		return nil, err
	}

	return &ResponseError{
		Reason: responseError.Error.Reason,
	}, nil
}

// Check if there was an response error.
func checkResponseIsError(res *esapi.Response, ignoreIf ...string) error {
	if res.IsError() {
		errMsg, err := parseResponseBodyError(res.Body)
		if err != nil {
			return err
		}

		opts := []customerror.Option{
			customerror.WithStatusCode(res.StatusCode),
		}

		if ignoreIf != nil {
			opts = append(opts, customerror.WithIgnoreString(ignoreIf...))
		}

		return customerror.New(errMsg.Reason, opts...)
	}

	return nil
}
