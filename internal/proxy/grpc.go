package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/middleware"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

var grpcCodec = proxyproto.Codec{}

type rpcCredentials struct {
	key   string
	value string
}

func (t rpcCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		t.key: t.value,
	}, nil
}

func (t rpcCredentials) RequireTransportSecurity() bool {
	return false
}

func getGrpcHost(endpoint string) (string, error) {
	var host string
	if strings.HasPrefix(endpoint, "grpc://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		host = u.Host
	} else {
		host = endpoint
	}
	return host, nil
}

func getDialOpts(name string, p Config) ([]grpc.DialOption, error) {
	var dialOpts []grpc.DialOption
	if p.GRPC.CredentialsKey != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&rpcCredentials{
			key:   p.GRPC.CredentialsKey,
			value: p.GRPC.CredentialsValue,
		}))
	}
	if p.GRPC.TLS.Enabled {
		tlsConfig, err := p.GRPC.TLS.ToGoTLSConfig("proxy_grpc:" + name)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config %v", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if p.GRPC.Compression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	if p.TestGrpcDialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(p.TestGrpcDialer))
	}

	return dialOpts, nil
}

func grpcRequestContext(ctx context.Context, proxy Config) context.Context {
	md := requestMetadata(ctx, proxy.HttpHeaders, proxy.GrpcMetadata)
	return metadata.NewOutgoingContext(ctx, md)
}

func httpRequestHeaders(ctx context.Context, proxy Config) http.Header {
	return requestHeaders(ctx, proxy.HttpHeaders, proxy.GrpcMetadata, proxy.HTTP.StaticHeaders)
}

func requestMetadata(ctx context.Context, allowedHeaders []string, allowedMetaKeys []string) metadata.MD {
	requestMD := metadata.MD{}
	emulatedHeaders, _ := clientcontext.GetEmulatedHeadersFromContext(ctx)
	for k, v := range emulatedHeaders {
		if slices.Contains(allowedHeaders, strings.ToLower(k)) {
			requestMD.Set(k, v)
		}
	}
	if headers, ok := middleware.GetHeadersFromContext(ctx); ok {
		for k, vv := range headers {
			if slices.Contains(allowedHeaders, strings.ToLower(k)) {
				requestMD.Set(k, vv...)
			}
		}
		return requestMD
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for k, vv := range md {
		if slices.Contains(allowedMetaKeys, k) {
			requestMD[k] = vv
		}
	}
	return requestMD
}

func requestHeaders(ctx context.Context, allowedHeaders []string, allowedMetaKeys []string, staticHeaders map[string]string) http.Header {
	emulatedHeaders, _ := clientcontext.GetEmulatedHeadersFromContext(ctx)
	if headers, ok := middleware.GetHeadersFromContext(ctx); ok {
		return getProxyHeader(headers, allowedHeaders, staticHeaders, emulatedHeaders)
	}
	headers := http.Header{}
	for k, v := range staticHeaders {
		headers.Set(k, v)
	}
	for k, v := range emulatedHeaders {
		if slices.Contains(allowedHeaders, strings.ToLower(k)) {
			headers.Set(k, v)
		}
	}
	headers.Set("Content-Type", "application/json")
	md, _ := metadata.FromIncomingContext(ctx)
	for k, vv := range md {
		if slices.Contains(allowedMetaKeys, k) {
			headers[k] = vv
		}
	}
	return headers
}
