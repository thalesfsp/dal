package elasticsearch

// ResponseSourceFromES is the data from the Elasticsearch response.
type ResponseSourceFromES struct {
	Data interface{} `json:"_source"`
}

// ResponseErrorFromESReason is the reason from the Elasticsearch response.
type ResponseErrorFromESReason struct {
	Reason string `json:"reason"`
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
	TrackTotalHits bool `json:"track_total_hits" default:"false" query:"track_total_hits"`
}
