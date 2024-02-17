package dto

type BasicGetResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"value"`
}
