package dtos

// Impression struct to map an impression
type Impression struct {
	KeyName      string `json:"k"`
	BucketingKey string `json:"b"`
	FeatureName  string `json:"f"`
	Treatment    string `json:"t"`
	Label        string `json:"r"`
	ChangeNumber int64  `json:"c"`
	Time         int64  `json:"m"`
}

// ImpressionQueueObject struct mapping impressions
type ImpressionQueueObject struct {
	Metadata   Metadata   `json:"m"`
	Impression Impression `json:"i"`
}
