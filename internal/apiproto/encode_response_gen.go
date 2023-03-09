// Code generated by internal/gen/api/main.go. DO NOT EDIT.

package apiproto

import "encoding/json"

// JSONResponseEncoder ...
type JSONResponseEncoder struct{}

func NewJSONResponseEncoder() *JSONResponseEncoder {
	return &JSONResponseEncoder{}
}

// EncodeBatch ...
func (e *JSONResponseEncoder) EncodeBatch(response *BatchResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodePublish ...
func (e *JSONResponseEncoder) EncodePublish(response *PublishResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeBroadcast ...
func (e *JSONResponseEncoder) EncodeBroadcast(response *BroadcastResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeSubscribe ...
func (e *JSONResponseEncoder) EncodeSubscribe(response *SubscribeResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeUnsubscribe ...
func (e *JSONResponseEncoder) EncodeUnsubscribe(response *UnsubscribeResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeDisconnect ...
func (e *JSONResponseEncoder) EncodeDisconnect(response *DisconnectResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodePresence ...
func (e *JSONResponseEncoder) EncodePresence(response *PresenceResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodePresenceStats ...
func (e *JSONResponseEncoder) EncodePresenceStats(response *PresenceStatsResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeHistory ...
func (e *JSONResponseEncoder) EncodeHistory(response *HistoryResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeHistoryRemove ...
func (e *JSONResponseEncoder) EncodeHistoryRemove(response *HistoryRemoveResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeInfo ...
func (e *JSONResponseEncoder) EncodeInfo(response *InfoResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeRPC ...
func (e *JSONResponseEncoder) EncodeRPC(response *RPCResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeRefresh ...
func (e *JSONResponseEncoder) EncodeRefresh(response *RefreshResponse) ([]byte, error) {
	return json.Marshal(response)
}

// EncodeChannels ...
func (e *JSONResponseEncoder) EncodeChannels(response *ChannelsResponse) ([]byte, error) {
	return json.Marshal(response)
}