package fetch_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbarn/bb-remote-asset/internal/mock"
	"github.com/buildbarn/bb-remote-asset/pkg/fetch"
	pb "github.com/buildbarn/bb-remote-asset/pkg/proto/configuration/bb_remote_asset/fetch"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// TestBrokerCredentialEndToEnd exercises the full FetchBlob path with a
// broker-configured fetcher: broker /delegate + /token → HTTP fetch
// with injected auth → CAS Put. This is the path that failed with EOF
// in production.
func TestBrokerCredentialEndToEnd(t *testing.T) {
	ctrl := gomock.NewController(t)

	delegateCalled := false
	tokenCalled := false
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delegate":
			delegateCalled = true
			require.Equal(t, "Bearer client-jwt", r.Header.Get("Authorization"))
			fmt.Fprintf(w, `{"nonce":"nonce-abc"}`)
		case "/token":
			tokenCalled = true
			fmt.Fprintf(w, `{"token":"upstream-token","scheme":"bearer"}`)
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

	fetcher := fetch.NewHTTPFetcher(
		&http.Client{Transport: roundTripper},
		casBlobAccess,
		broker.URL,
		[]*pb.FetcherConfiguration_BrokerCredentialMapping{
			{Host: "private-registry.example.com", Destination: "artifactory"},
		},
	)

	md := metadata.Pairs("authorization", "Bearer client-jwt")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	request := &remoteasset.FetchBlobRequest{
		InstanceName:   InstanceName,
		Uris:           []string{"https://private-registry.example.com/pkg/v1.0.tar.gz"},
		Qualifiers:     []*remoteasset.Qualifier{},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	roundTripper.EXPECT().RoundTrip(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "Bearer upstream-token", req.Header.Get("Authorization"))
		return &http.Response{
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
			ContentLength: int64(len(TestData)),
		}, nil
	})
	casBlobAccess.EXPECT().Put(gomock.Any(), helloDigest, gomock.Any()).Return(nil)

	response, err := fetcher.FetchBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int32(codes.OK), response.Status.Code)
	require.True(t, delegateCalled, "broker /delegate was not called")
	require.True(t, tokenCalled, "broker /token was not called")
}

// TestBrokerNotCalledForUnmappedHost verifies that the broker is not
// contacted for URLs whose host has no broker mapping.
func TestBrokerNotCalledForUnmappedHost(t *testing.T) {
	ctrl := gomock.NewController(t)

	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("broker should not have been called")
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

	fetcher := fetch.NewHTTPFetcher(
		&http.Client{Transport: roundTripper},
		casBlobAccess,
		broker.URL,
		[]*pb.FetcherConfiguration_BrokerCredentialMapping{
			{Host: "private-registry.example.com", Destination: "artifactory"},
		},
	)

	md := metadata.Pairs("authorization", "Bearer client-jwt")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	request := &remoteasset.FetchBlobRequest{
		InstanceName:   InstanceName,
		Uris:           []string{"https://github.com/nanopb/nanopb/archive/abc.tar.gz"},
		Qualifiers:     []*remoteasset.Qualifier{},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
		StatusCode:    200,
		Body:          io.NopCloser(bytes.NewBuffer([]byte(TestData))),
		ContentLength: int64(len(TestData)),
	}, nil)
	casBlobAccess.EXPECT().Put(gomock.Any(), helloDigest, gomock.Any()).Return(nil)

	response, err := fetcher.FetchBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int32(codes.OK), response.Status.Code)
}
