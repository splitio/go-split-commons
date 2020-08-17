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

// ImpressionDTO struct to map an impression
type ImpressionDTO struct {
	KeyName      string `json:"keyName"`
	Treatment    string `json:"treatment"`
	Time         int64  `json:"time"`
	ChangeNumber int64  `json:"changeNumber"`
	Label        string `json:"label"`
	BucketingKey string `json:"bucketingKey,omitempty"`
	Pt           int64  `json:"pt,omitempty"`
}

// ImpressionsDTO struct mapping impressions to post
type ImpressionsDTO struct {
	TestName       string          `json:"testName"`
	KeyImpressions []ImpressionDTO `json:"keyImpressions"`
}
