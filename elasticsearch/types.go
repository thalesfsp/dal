package elasticsearch

// ResponseSourceFromES is the data from the Elasticsearch response.
type ResponseSourceFromES struct {
	Data interface{} `json:"_source"`
}

// ResponseErrorFromESReason is the reason from the Elasticsearch response.
//
// Elasticsearch's error envelope can carry chained causation via `caused_by`
// (recursive) and `root_cause` (array of leaf causes). Before v2.1.1 only the
// top-level `reason` was surfaced, which for `search_phase_execution_exception`
// is a meaningless "all shards failed" string while the actionable detail
// lives in `caused_by.reason` (e.g. "Result window is too large..."). Both
// chains are now captured so callers — directly via the typed fields or
// indirectly via the enriched `ResponseError.Reason` string — can see the
// real cause.
type ResponseErrorFromESReason struct {
	Type      string                      `json:"type,omitempty"`
	Reason    string                      `json:"reason"`
	CausedBy  *ResponseErrorFromESReason  `json:"caused_by,omitempty"`
	RootCause []ResponseErrorFromESReason `json:"root_cause,omitempty"`
}

// ResponseErrorFromES is the error from the Elasticsearch response.
type ResponseErrorFromES struct {
	Error  ResponseErrorFromESReason `json:"error"`
	Status int                       `json:"status"`
}

// ResponseError is the error from the Elasticsearch response.
type ResponseError struct {
	Reason string `json:"reason"`
	Status int    `json:"status"`
}

// CountResponse is the response from the Count operation.
type CountResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore any   `json:"max_score"`
		Hits     []any `json:"hits"`
	} `json:"hits"`
}

// ListAny is a struct for the `list.List` `Any` field.
type ListAny struct {
	// TrackTotalHits indicates whether Elasticsearch should track the true
	// total number of hits.
	TrackTotalHits bool `default:"false" json:"track_total_hits" query:"track_total_hits"`
}
