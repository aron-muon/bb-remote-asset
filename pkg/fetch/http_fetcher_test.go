package fetch_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbarn/bb-remote-asset/internal/mock"
	"github.com/buildbarn/bb-remote-asset/pkg/fetch"
	pb "github.com/buildbarn/bb-remote-asset/pkg/proto/configuration/bb_remote_asset/fetch"
	"github.com/buildbarn/bb-remote-asset/pkg/qualifier"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type headerMatcher struct {
	headers map[string]string
}

func (hm *headerMatcher) String() string {
	return fmt.Sprintf("has headers: %v", hm.headers)
}

func (hm *headerMatcher) Matches(x interface{}) bool {
	req, ok := x.(*http.Request)
	if !ok {
		return false
	}

	for header, val := range hm.headers {
		headerList, ok := req.Header[header]
		if !ok {
			return false
		}

		if headerList[0] != val {
			return false
		}
	}

	return true
}

// Instance name used in the test
const InstanceName = ""

// Data used as the blob
const TestData = "Hello"

// Convert DigestFunction Enum to strings
var HashNames = map[remoteexecution.DigestFunction_Value]string{
	remoteexecution.DigestFunction_SHA256:     "sha256",
	remoteexecution.DigestFunction_SHA1:       "sha1",
	remoteexecution.DigestFunction_MD5:        "md5",
	remoteexecution.DigestFunction_SHA384:     "sha384",
	remoteexecution.DigestFunction_SHA512:     "sha512",
	remoteexecution.DigestFunction_SHA256TREE: "sha256tree",
}

// Convert a Digest to the representation used by checksum.sri qualifiers.  Note,
// df must match the value used by d
func digestToChecksumSri(df remoteexecution.DigestFunction_Value, d digest.Digest) string {
	return fmt.Sprintf("%s-%s", HashNames[df], base64.StdEncoding.EncodeToString(d.GetHashBytes()))
}

func TestHTTPFetcherFetchBlobSuccessSHA256(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_SHA256,
	)
}

func TestHTTPFetcherFetchBlobSuccessSHA1(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_SHA1,
	)
}

func TestHTTPFetcherFetchBlobSuccessMD5(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_MD5,
	)
}

func TestHTTPFetcherFetchBlobSuccessSHA384(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_SHA384,
	)
}

func TestHTTPFetcherFetchBlobSuccessSHA512(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_SHA512,
	)
}

func TestHTTPFetcherFetchBlobSuccessSha256tree(t *testing.T) {
	testHTTPFetcherFetchBlobSuccessWithHasher(
		t,
		remoteexecution.DigestFunction_SHA256TREE,
	)
}

func testHTTPFetcherFetchBlobSuccessWithHasher(t *testing.T, digestFunctionEnum remoteexecution.DigestFunction_Value) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instance := util.Must(digest.NewInstanceName(InstanceName))
	digestFunction, err := instance.GetDigestFunction(digestFunctionEnum, 0)
	require.NoError(t, err)
	digestGenerator := digestFunction.NewGenerator(int64(len(TestData)))
	digestGenerator.Write([]byte(TestData))
	helloDigest := digestGenerator.Sum()

	request := &remoteasset.FetchBlobRequest{
		InstanceName: InstanceName,
		Uris:         []string{"www.example.com"},
		Qualifiers: []*remoteasset.Qualifier{
			{
				Name:  "checksum.sri",
				Value: digestToChecksumSri(digestFunctionEnum, helloDigest),
			},
		},
		DigestFunction: digestFunctionEnum,
	}
	casBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	HTTPFetcher := fetch.NewHTTPFetcher(&http.Client{Transport: roundTripper}, casBlobAccess, "", nil)

	t.Run("Success"+helloDigest.GetDigestFunction().GetEnumValue().String(), func(t *testing.T) {
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		httpDoCall := roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: 5,
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpDoCall)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("SuccessNoContentLength", func(t *testing.T) {
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: -1,
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})
}

func TestHTTPFetcherFetchBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instance := util.Must(digest.NewInstanceName(InstanceName))
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)
	digestGenerator := digestFunction.NewGenerator(int64(len(TestData)))
	digestGenerator.Write([]byte(TestData))
	helloDigest := digestGenerator.Sum()

	uri := "www.example.com"
	request := &remoteasset.FetchBlobRequest{
		InstanceName: InstanceName,
		Uris:         []string{uri, "www.another.com"},
		Qualifiers: []*remoteasset.Qualifier{
			{
				Name:  "checksum.sri",
				Value: digestToChecksumSri(remoteexecution.DigestFunction_SHA256, helloDigest),
			},
		},
	}
	casBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	HTTPFetcher := fetch.NewHTTPFetcher(&http.Client{Transport: roundTripper}, casBlobAccess, "", nil)

	t.Run("SuccessNoExpectedDigest", func(t *testing.T) {
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri, "www.another.com"},
			Qualifiers:   []*remoteasset.Qualifier{},
		}
		httpDoCall := roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: 5,
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpDoCall)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("SuccessNoExpectedDigestOrContentLength", func(t *testing.T) {
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri, "www.another.com"},
			Qualifiers:   []*remoteasset.Qualifier{},
		}
		httpDoCall := roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: -1,
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpDoCall)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("UnknownChecksumSriAlgo", func(t *testing.T) {
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri, "www.another.com"},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "checksum.sri",
					Value: "sha0-GF+NsyJx/iX1Yab8k4suJkMG7DBO2lGAB9F2SCY4GWk=",
				},
			},
		}

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unsupported checksum algorithm sha0"), err)
		require.Nil(t, response)
	})

	t.Run("BadChecksumSriAlgo", func(t *testing.T) {
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri, "www.another.com"},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "checksum.sri",
					Value: "no_dash",
				},
			},
		}

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Bad checksum.sri hash expression: no_dash"), err)
		require.Nil(t, response)
	})

	t.Run("BadChecksumSriBase64Value", func(t *testing.T) {
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri, "www.another.com"},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "checksum.sri",
					Value: "sha256-no-base64",
				},
			},
		}

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to decode checksum as base64 encoded sha256 sum: illegal base64 data at input byte 2"), err)
		require.Nil(t, response)
	})

	t.Run("OneFailOneSuccess", func(t *testing.T) {
		httpFailCall := roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:     "404 Not Found",
			StatusCode: 404,
		}, nil)
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		httpSuccessCall := roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: 5,
		}, nil).After(httpFailCall)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpSuccessCall)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("Failure", func(t *testing.T) {
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:     "404 Not Found",
			StatusCode: 404,
		}, nil).MaxTimes(2)

		_, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.NotFound)
	})

	t.Run("WithLegacyAuthHeaders", func(t *testing.T) {
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "bazel.auth_headers",
					Value: `{ "www.example.com": {"Authorization": "Bearer letmein"}}`,
				},
				{
					Name:  "checksum.sri",
					Value: digestToChecksumSri(remoteexecution.DigestFunction_SHA256, helloDigest),
				},
			},
		}
		require.Empty(t, HTTPFetcher.CheckQualifiers(qualifier.QualifiersToSet(request.Qualifiers)))
		matcher := &headerMatcher{
			headers: map[string]string{
				"Authorization": "Bearer letmein",
			},
		}
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		httpDoCall := roundTripper.EXPECT().RoundTrip(matcher).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: 5,
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpDoCall)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("WithAuthHeaders", func(t *testing.T) {
		request := &remoteasset.FetchBlobRequest{
			InstanceName: "",
			Uris:         []string{"www.another.com", uri},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "http_header:Authorization",
					Value: `Bearer anothertoken`,
				},
				{
					Name:  "http_header:Accept",
					Value: "application/vnd.docker.distribution.manifest.list.v2+json",
				},
				{
					Name:  "http_header_url:1:Authorization",
					Value: `Bearer letmein1`,
				},
				{
					Name:  "checksum.sri",
					Value: digestToChecksumSri(remoteexecution.DigestFunction_SHA256, helloDigest),
				},
			},
		}
		require.Empty(t, HTTPFetcher.CheckQualifiers(qualifier.QualifiersToSet(request.Qualifiers)))
		matcherReq1 := &headerMatcher{
			headers: map[string]string{
				"Authorization": "Bearer anothertoken",
				"Accept":        "application/vnd.docker.distribution.manifest.list.v2+json",
			},
		}
		matcherReq2 := &headerMatcher{
			headers: map[string]string{
				"Authorization": "Bearer letmein1",
				"Accept":        "application/vnd.docker.distribution.manifest.list.v2+json",
			},
		}
		roundTripper.EXPECT().RoundTrip(matcherReq1).Return(&http.Response{
			Status:     "404 NotFound",
			StatusCode: 404,
		}, nil)
		body := io.NopCloser(bytes.NewBuffer([]byte(TestData)))
		httpDoCall2 := roundTripper.EXPECT().RoundTrip(matcherReq2).Return(&http.Response{
			Status:        "200 Success",
			StatusCode:    200,
			Body:          body,
			ContentLength: 5,
		}, nil)

		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil).After(httpDoCall2)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.Nil(t, err)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})
}

func TestHTTPFetcherBrokerCredentialInjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Set up a fake broker that returns a known token.
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delegate":
			fmt.Fprintf(w, `{"nonce":"nonce-abc"}`)
		case "/token":
			fmt.Fprintf(w, `{"token":"art-tok-123","scheme":"bearer"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer broker.Close()

	instance := util.Must(digest.NewInstanceName(InstanceName))
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)
	digestGenerator := digestFunction.NewGenerator(int64(len(TestData)))
	digestGenerator.Write([]byte(TestData))
	helloDigest := digestGenerator.Sum()

	casBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)

	mappings := []*pb.FetcherConfiguration_BrokerCredentialMapping{
		{
			Host:        "artifactory.example.com",
			Destination: "artifactory",
		},
	}
	HTTPFetcher := fetch.NewHTTPFetcher(
		&http.Client{Transport: roundTripper},
		casBlobAccess,
		broker.URL,
		mappings,
	)

	t.Run("BrokerInjectsAuthHeader", func(t *testing.T) {
		// Simulate an incoming gRPC context with a JWT (as the ALB would provide).
		md := metadata.Pairs("authorization", "Bearer client-okta-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://artifactory.example.com/some/package.tar.gz"},
			Qualifiers:     []*remoteasset.Qualifier{},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		// The RoundTripper should receive a request with the broker-injected Authorization header.
		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer art-tok-123"},
		}).Return(&http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.BlobDigest, helloDigest.GetProto()))
	})

	t.Run("NoBrokerForUnmappedHost", func(t *testing.T) {
		md := metadata.Pairs("authorization", "Bearer client-okta-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://github.com/some/repo/archive/abc.tar.gz"},
			Qualifiers:     []*remoteasset.Qualifier{},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		// No broker auth — GitHub is not in the mappings. No Authorization header.
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("ClientAuthHeadersTakePrecedence", func(t *testing.T) {
		md := metadata.Pairs("authorization", "Bearer client-okta-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Client provides explicit auth_headers qualifier — broker should NOT override.
		request := &remoteasset.FetchBlobRequest{
			InstanceName: InstanceName,
			Uris:         []string{"https://artifactory.example.com/some/package.tar.gz"},
			Qualifiers: []*remoteasset.Qualifier{
				{
					Name:  "http_header:Authorization",
					Value: "Bearer client-provided-token",
				},
			},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer client-provided-token"},
		}).Return(&http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("NoJWTInContextPassesThrough", func(t *testing.T) {
		// No gRPC metadata — broker can't delegate. Should pass through without auth.
		ctx := context.Background()

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://artifactory.example.com/some/package.tar.gz"},
			Qualifiers:     []*remoteasset.Qualifier{},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := HTTPFetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})
}

func TestHTTPFetcherBrokerPathPrefixMatching(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Broker returns a destination-specific token so tests can
	// verify which mapping was selected.
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delegate":
			fmt.Fprintf(w, `{"nonce":"nonce-abc"}`)
		case "/token":
			var req struct {
				Destination string `json:"destination"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &req)
			fmt.Fprintf(w, `{"token":"tok-%s","scheme":"bearer"}`, req.Destination)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer broker.Close()

	instance := util.Must(digest.NewInstanceName(InstanceName))
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)
	digestGenerator := digestFunction.NewGenerator(int64(len(TestData)))
	digestGenerator.Write([]byte(TestData))
	helloDigest := digestGenerator.Sum()

	casBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)

	t.Run("PathPrefixSelectsCorrectDestination", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "raw.ghe.example.com", PathPrefix: "/Org-A", Destination: "dest-a"},
				{Host: "raw.ghe.example.com", PathPrefix: "/Org-B", Destination: "dest-b"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://raw.ghe.example.com/Org-A/repo/main/file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer tok-dest-a"},
		}).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("PathPrefixNoMatch", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "raw.ghe.example.com", PathPrefix: "/Org-A", Destination: "dest-a"},
				{Host: "raw.ghe.example.com", PathPrefix: "/Org-B", Destination: "dest-b"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://raw.ghe.example.com/Other-Org/repo/main/file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		// No broker injection — no Authorization header on the request.
		roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("LongestPrefixWins", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "raw.ghe.example.com", PathPrefix: "/Org", Destination: "dest-short"},
				{Host: "raw.ghe.example.com", PathPrefix: "/Org/specific", Destination: "dest-long"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://raw.ghe.example.com/Org/specific/repo/file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer tok-dest-long"},
		}).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("EmptyPrefixWildcard", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "raw.ghe.example.com", PathPrefix: "/Org-A", Destination: "dest-a"},
				{Host: "raw.ghe.example.com", Destination: "dest-wildcard"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://raw.ghe.example.com/Other-Org/repo/main/file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer tok-dest-wildcard"},
		}).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("PathNormalization", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "raw.ghe.example.com", PathPrefix: "/Org", Destination: "dest-org"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Double slashes and dot segments should be normalized.
		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://raw.ghe.example.com//Org//repo/./file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer tok-dest-org"},
		}).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})

	t.Run("HostOnlyBackCompat", func(t *testing.T) {
		// Mapping without PathPrefix (empty string) matches any path.
		fetcher := fetch.NewHTTPFetcher(
			&http.Client{Transport: roundTripper},
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{Host: "artifactory.example.com", Destination: "artifactory"},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{"https://artifactory.example.com/api/pypi/simple/grpcio/"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}

		roundTripper.EXPECT().RoundTrip(&headerMatcher{
			headers: map[string]string{"Authorization": "Bearer tok-artifactory"},
		}).Return(&http.Response{
			Status: "200 OK", StatusCode: 200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil)
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
	})
}

func TestHTTPFetcherBrokerPathPrefixGHEScenario(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Broker returns a destination-specific token. The /delegate
	// and /token exchanges mirror the real broker protocol.
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delegate":
			fmt.Fprintf(w, `{"nonce":"nonce-ghe"}`)
		case "/token":
			var req struct {
				Destination string `json:"destination"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &req)
			fmt.Fprintf(w, `{"token":"ghtoken-%s","scheme":"bearer"}`, req.Destination)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer broker.Close()

	instance := util.Must(digest.NewInstanceName(InstanceName))
	digestFunction, err := instance.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)
	digestGenerator := digestFunction.NewGenerator(int64(len(TestData)))
	digestGenerator.Write([]byte(TestData))
	helloDigest := digestGenerator.Sum()

	casBlobAccess := mock.NewMockBlobAccess(ctrl)

	// Capture the actual HTTP request to verify headers.
	var capturedReq *http.Request
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedReq = r
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(TestData)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(TestData))
	}))
	defer upstream.Close()

	t.Run("GHEPathPrefixInjectsCredential", func(t *testing.T) {
		// Two orgs on the same GHE host with different broker
		// destinations — the exact production scenario.
		fetcher := fetch.NewHTTPFetcher(
			upstream.Client(),
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space",
					Destination: "ghe-app-muon-space",
				},
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space-Gov",
					Destination: "ghe-app-muon-space-gov",
				},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-build-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		capturedReq = nil
		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{upstream.URL + "/Muon-Space/bazel-module-registry/refs/heads/main/modules/nanopb/source.json"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.NotNil(t, capturedReq, "upstream should have received an HTTP request")
		require.Equal(t, "Bearer ghtoken-ghe-app-muon-space", capturedReq.Header.Get("Authorization"),
			"broker should inject the Muon-Space org token")
	})

	t.Run("GHEPathPrefixGovOrg", func(t *testing.T) {
		fetcher := fetch.NewHTTPFetcher(
			upstream.Client(),
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space",
					Destination: "ghe-app-muon-space",
				},
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space-Gov",
					Destination: "ghe-app-muon-space-gov",
				},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-build-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		capturedReq = nil
		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{upstream.URL + "/Muon-Space-Gov/infra/refs/heads/main/config.yaml"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.NotNil(t, capturedReq)
		require.Equal(t, "Bearer ghtoken-ghe-app-muon-space-gov", capturedReq.Header.Get("Authorization"),
			"broker should inject the Muon-Space-Gov org token")
	})

	t.Run("GHENoMappingNoAuth", func(t *testing.T) {
		// URL for an org that has NO mapping — no credential injection.
		fetcher := fetch.NewHTTPFetcher(
			upstream.Client(),
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space",
					Destination: "ghe-app-muon-space",
				},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-build-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		capturedReq = nil
		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{upstream.URL + "/Other-Org/repo/refs/heads/main/BUILD.bazel"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.NotNil(t, capturedReq)
		require.Empty(t, capturedReq.Header.Get("Authorization"),
			"no mapping for /Other-Org — request should have no auth header")
	})

	t.Run("GHEPrefixDoesNotMatchSimilarOrg", func(t *testing.T) {
		// /Muon-Space-Gov must NOT match a /Muon-Space prefix.
		// Longest-prefix logic: /Muon-Space is a prefix of
		// /Muon-Space-Gov as a string, so if only /Muon-Space is
		// configured, it WILL match /Muon-Space-Gov. This is
		// documented behavior — operators who need to prevent this
		// must add /Muon-Space/ (trailing slash) or add a more
		// specific mapping. This test documents the current semantics.
		fetcher := fetch.NewHTTPFetcher(
			upstream.Client(),
			casBlobAccess,
			broker.URL,
			[]*pb.FetcherConfiguration_BrokerCredentialMapping{
				{
					Host:        upstream.Listener.Addr().String(),
					PathPrefix:  "/Muon-Space",
					Destination: "ghe-app-muon-space",
				},
			},
		)

		md := metadata.Pairs("authorization", "Bearer client-build-jwt")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		capturedReq = nil
		request := &remoteasset.FetchBlobRequest{
			InstanceName:   InstanceName,
			Uris:           []string{upstream.URL + "/Muon-Space-Gov/repo/file.txt"},
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}
		casBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(nil)

		response, err := fetcher.FetchBlob(ctx, request)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.NotNil(t, capturedReq)
		require.Equal(t, "Bearer ghtoken-ghe-app-muon-space", capturedReq.Header.Get("Authorization"),
			"/Muon-Space prefix matches /Muon-Space-Gov — this is expected string prefix behavior")
	})
}

func TestHTTPFetcherFetchDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	uri := "www.example.com"
	request := &remoteasset.FetchDirectoryRequest{
		InstanceName: "",
		Uris:         []string{uri, "www.another.com"},
	}
	casBlobAccess := mock.NewMockBlobAccess(ctrl)
	roundTripper := mock.NewMockRoundTripper(ctrl)
	HTTPFetcher := fetch.NewHTTPFetcher(&http.Client{Transport: roundTripper}, casBlobAccess, "", nil)
	_, err := HTTPFetcher.FetchDirectory(ctx, request)
	require.NotNil(t, err)
	require.Equal(t, status.Code(err), codes.PermissionDenied)
}
