package elasticsearch

import (
	"io"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/shared"
)

// maxCausedByDepth bounds the recursion when walking nested `caused_by`
// frames *below* the top-level error. Counting the top reason itself the
// surfaced chain can therefore be up to maxCausedByDepth+1 entries long.
// ES responses don't legitimately exceed this depth; the cap is purely a
// safety net against pathological/malformed payloads.
const maxCausedByDepth = 15

// Parse ES response body.
func parseResponseBody(r io.Reader, v any) error {
	return shared.Decode(r, v)
}

// Parse ES response body when it's an error.
//
// As of v2.1.1, the surfaced `ResponseError.Reason` now includes the full
// `caused_by` chain and the first `root_cause` entry, joined with `: `.
// This preserves the top-level reason text (backward-compatible for
// substring matchers) while exposing the actionable detail that was
// previously dropped — most notably for `search_phase_execution_exception`
// errors where the top-level reason is "all shards failed" and the real
// cause is nested.
func parseResponseBodyError(r io.Reader) (*ResponseError, error) {
	responseError := &ResponseErrorFromES{}
	if err := parseResponseBody(r, responseError); err != nil {
		return nil, err
	}

	return &ResponseError{
		Reason: buildReasonChain(responseError.Error),
		Status: responseError.Status,
	}, nil
}

// buildReasonChain renders a human-readable error chain from the ES error
// envelope. The top-level reason is always first so existing callers that
// match on the original substring continue to work.
func buildReasonChain(top ResponseErrorFromESReason) string {
	parts := make([]string, 0, 4)
	if top.Reason != "" {
		parts = append(parts, top.Reason)
	}

	cause := top.CausedBy
	for depth := 0; cause != nil && depth < maxCausedByDepth; depth++ {
		if cause.Reason != "" && !containsString(parts, cause.Reason) {
			parts = append(parts, cause.Reason)
		}
		cause = cause.CausedBy
	}

	// Include the first root_cause if it adds information not already in
	// the chain. ES often duplicates root_cause[0] inside caused_by, so we
	// dedup to avoid noisy repetition.
	if len(top.RootCause) > 0 {
		if rc := top.RootCause[0].Reason; rc != "" && !containsString(parts, rc) {
			parts = append(parts, rc)
		}
	}

	return strings.Join(parts, ": ")
}

// containsString returns true if needle is already in haystack. Linear scan
// is fine — the chain is bounded by maxCausedByDepth and rootCause[0].
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
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
