package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/centrifugal/centrifugo/v5/internal/tools"
)

type TransformError struct {
	Code      uint32 `mapstructure:"code" json:"code"`
	Message   string `mapstructure:"message" json:"message"`
	Temporary bool   `mapstructure:"temporary" json:"temporary"`
}

type TransformDisconnect struct {
	Code   uint32 `mapstructure:"code" json:"code"`
	Reason string `mapstructure:"reason" json:"reason"`
}

type HttpStatusToCodeTransform struct {
	StatusCode   int                 `mapstructure:"status_code" json:"status_code"`
	ToError      TransformError      `mapstructure:"to_error" json:"to_error"`
	ToDisconnect TransformDisconnect `mapstructure:"to_disconnect" json:"to_disconnect"`
}

func (t *HttpStatusToCodeTransform) Validate() error {
	if t.StatusCode == 0 {
		return errors.New("no status code specified")
	}
	if t.ToDisconnect.Code == 0 && t.ToError.Code == 0 {
		return errors.New("no error or disconnect code set")
	}
	if t.ToDisconnect.Code > 0 && t.ToError.Code > 0 {
		return errors.New("only error or disconnect code can be set")
	}
	if !tools.IsASCII(t.ToDisconnect.Reason) {
		return errors.New("disconnect reason must be ASCII")
	}
	if !tools.IsASCII(t.ToError.Message) {
		return errors.New("error message must be ASCII")
	}
	const reasonOrMessageMaxLength = 123 // limit comes from WebSocket close reason length limit. See https://datatracker.ietf.org/doc/html/rfc6455.
	if len(t.ToDisconnect.Reason) > reasonOrMessageMaxLength {
		return fmt.Errorf("disconnect reason can be up to %d characters long", reasonOrMessageMaxLength)
	}
	return nil
}

// Config for proxy.
type Config struct {
	// Name is a unique name of proxy to reference.
	Name string `mapstructure:"name" json:"name"`
	// Endpoint - HTTP address or GRPC service endpoint.
	Endpoint string `mapstructure:"endpoint" json:"endpoint"`
	// Timeout for proxy request.
	Timeout tools.Duration `mapstructure:"timeout" json:"timeout,omitempty"`

	// HTTPHeaders is a list of HTTP headers to proxy. No headers used by proxy by default.
	// If GRPC proxy is used then request HTTP headers set to outgoing request metadata.
	HttpHeaders          []string                    `mapstructure:"http_headers" json:"http_headers,omitempty"`
	HttpStatusTransforms []HttpStatusToCodeTransform `mapstructure:"http_status_to_code_transforms" json:"http_status_to_code_transforms,omitempty"`

	// GRPCMetadata is a list of GRPC metadata keys to proxy. No meta keys used by proxy by
	// default. If HTTP proxy is used then these keys become outgoing request HTTP headers.
	GrpcMetadata []string `mapstructure:"grpc_metadata" json:"grpc_metadata,omitempty"`

	// StaticHttpHeaders is a static set of key/value pairs to attach to HTTP proxy request as
	// headers. Headers received from HTTP client request or metadata from GRPC client request
	// both have priority over values set in StaticHttpHeaders map.
	StaticHttpHeaders map[string]string `mapstructure:"static_http_headers" json:"static_http_headers,omitempty"`

	// BinaryEncoding makes proxy send data as base64 string (assuming it contains custom
	// non-JSON payload).
	BinaryEncoding bool `mapstructure:"binary_encoding" json:"binary_encoding,omitempty"`
	// IncludeConnectionMeta to each proxy request (except connect where it's obtained).
	IncludeConnectionMeta bool `mapstructure:"include_connection_meta" json:"include_connection_meta,omitempty"`

	// GrpcTLS is a common configuration for GRPC TLS.
	GrpcTLS tools.TLSConfig `mapstructure:"grpc_tls" json:"grpc_tls,omitempty"`
	// GrpcCertFile is a path to GRPC cert file on disk.
	GrpcCertFile string `mapstructure:"grpc_cert_file" json:"grpc_cert_file,omitempty"`
	// GrpcCredentialsKey is a custom key to add into per-RPC credentials.
	GrpcCredentialsKey string `mapstructure:"grpc_credentials_key" json:"grpc_credentials_key,omitempty"`
	// GrpcCredentialsValue is a custom value for GrpcCredentialsKey.
	GrpcCredentialsValue string `mapstructure:"grpc_credentials_value" json:"grpc_credentials_value,omitempty"`
	// GrpcCompression enables compression for outgoing calls (gzip).
	GrpcCompression bool `mapstructure:"grpc_compression" json:"grpc_compression,omitempty"`

	testGrpcDialer func(context.Context, string) (net.Conn, error)
}

func getEncoding(useBase64 bool) string {
	if useBase64 {
		return "binary"
	}
	return "json"
}

func isHttpEndpoint(endpoint string) bool {
	return strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://")
}

func GetConnectProxy(p Config) (ConnectProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPConnectProxy(p)
	}
	return NewGRPCConnectProxy(p)
}

func GetRefreshProxy(p Config) (RefreshProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPRefreshProxy(p)
	}
	return NewGRPCRefreshProxy(p)
}

func GetRpcProxy(p Config) (RPCProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPRPCProxy(p)
	}
	return NewGRPCRPCProxy(p)
}

func GetSubRefreshProxy(p Config) (SubRefreshProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPSubRefreshProxy(p)
	}
	return NewGRPCSubRefreshProxy(p)
}

func GetPublishProxy(p Config) (PublishProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPPublishProxy(p)
	}
	return NewGRPCPublishProxy(p)
}

func GetSubscribeProxy(p Config) (SubscribeProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPSubscribeProxy(p)
	}
	return NewGRPCSubscribeProxy(p)
}

func GetCacheEmptyProxy(p Config) (CacheEmptyProxy, error) {
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPCacheEmptyProxy(p)
	}
	return NewGRPCCacheEmptyProxy(p)
}

type PerCallData struct {
	Meta json.RawMessage
}
