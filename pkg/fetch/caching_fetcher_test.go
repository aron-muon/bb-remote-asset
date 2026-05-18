package fetch_test

import (
	"context"
	"testing"
	"time"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-asset/internal/mock"
	"github.com/buildbarn/bb-remote-asset/pkg/fetch"
	"github.com/buildbarn/bb-remote-asset/pkg/proto/asset"
	"github.com/buildbarn/bb-remote-asset/pkg/storage"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_digest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	protostatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFetchBlobCaching(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := bb_digest.NewInstanceName("")
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)

	uri := "www.example.com"
	request := &remoteasset.FetchBlobRequest{
		InstanceName: "",
		Uris:         []string{uri},
	}
	blobDigest := &remoteexecution.Digest{Hash: "d0d829c4c0ce64787cb1c998a9c29a109f8ed005633132fda4f29982487b04db", SizeBytes: 123}
	_, refDigest, err := storage.ProtoSerialise(storage.NewAssetReference([]string{uri}, []*remoteasset.Qualifier{}), digestFunction)
	require.NoError(t, err)

	t.Logf("Ref digest was %v", refDigest)

	backend := mock.NewMockBlobAccess(ctrl)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	mockFetcher := mock.NewMockFetcher(ctrl)
	cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

	t.Run("Success", func(t *testing.T) {
		backendGetCall := backend.EXPECT().Get(ctx, refDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		fetchBlobCall := mockFetcher.EXPECT().FetchBlob(ctx, request).Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Success!").Proto(),
			Uri:        uri,
			BlobDigest: blobDigest,
		}, nil).After(backendGetCall)
		backend.EXPECT().Put(ctx, refDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest bb_digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&asset.Asset{}, 1000)
				require.NoError(t, err)
				a := m.(*asset.Asset)
				require.True(t, proto.Equal(a.Digest, blobDigest))
				require.Equal(t, asset.Asset_BLOB, a.Type)
				return nil
			}).After(fetchBlobCall)
		response, err := cachingFetcher.FetchBlob(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("Failure", func(t *testing.T) {
		backendGetCall := backend.EXPECT().Get(ctx, gomock.Any()).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		mockFetcher.EXPECT().FetchBlob(ctx, request).Return(nil, status.Error(codes.NotFound, "Not Found!")).After(backendGetCall)
		_, err := cachingFetcher.FetchBlob(ctx, request)
		require.NotNil(t, err)
	})

	t.Run("Cached", func(t *testing.T) {
		backend.EXPECT().Get(ctx, refDigest).Return(buffer.NewProtoBufferFromProto(storage.NewBlobAsset(blobDigest, nil), buffer.UserProvided))
		response, err := cachingFetcher.FetchBlob(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})
}

func TestFetchDirectoryCaching(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := bb_digest.NewInstanceName("")
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)

	uri := "www.example.com"
	request := &remoteasset.FetchDirectoryRequest{
		InstanceName: "",
		Uris:         []string{uri},
	}
	dirDigest := &remoteexecution.Digest{Hash: "d0d829c4c0ce64787cb1c998a9c29a109f8ed005633132fda4f29982487b04db", SizeBytes: 123}
	_, refDigest, err := storage.ProtoSerialise(storage.NewAssetReference([]string{uri}, []*remoteasset.Qualifier{}), digestFunction)
	require.NoError(t, err)

	backend := mock.NewMockBlobAccess(ctrl)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	mockFetcher := mock.NewMockFetcher(ctrl)
	cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

	t.Run("Success", func(t *testing.T) {
		backendGetCall := backend.EXPECT().Get(ctx, refDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Directory not found")))
		fetchDirectoryCall := mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Success!").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: dirDigest,
		}, nil).After(backendGetCall)
		backend.EXPECT().Put(ctx, refDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest bb_digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&asset.Asset{}, 1000)
				require.NoError(t, err)
				a := m.(*asset.Asset)
				require.True(t, proto.Equal(a.Digest, dirDigest))
				require.Equal(t, asset.Asset_DIRECTORY, a.Type)
				return nil
			}).After(fetchDirectoryCall)
		response, err := cachingFetcher.FetchDirectory(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("Failure", func(t *testing.T) {
		backendGetCall := backend.EXPECT().Get(ctx, gomock.Any()).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Directory not found")))
		mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(nil, status.Error(codes.NotFound, "Not Found!")).After(backendGetCall)
		_, err := cachingFetcher.FetchDirectory(ctx, request)
		require.NotNil(t, err)
	})

	t.Run("Cached", func(t *testing.T) {
		backend.EXPECT().Get(ctx, refDigest).Return(buffer.NewProtoBufferFromProto(storage.NewBlobAsset(dirDigest, nil), buffer.UserProvided))
		response, err := cachingFetcher.FetchDirectory(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})
}

func TestCachingFetcherExpiry(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := digest.NewInstanceName("foo")
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)

	uri := "https://example.com/example.tar.gz"
	request := &remoteasset.FetchBlobRequest{
		InstanceName: "foo",
		Uris:         []string{uri},
	}
	_, refDigest, err := storage.ProtoSerialise(storage.NewAssetReference([]string{uri}, []*remoteasset.Qualifier{}), digestFunction)
	require.NoError(t, err)

	backend := mock.NewMockBlobAccess(ctrl)
	buf := buffer.NewProtoBufferFromProto(&asset.Asset{
		Digest: &remoteexecution.Digest{
			Hash:      "d1bc8d3ba4afc7e109612cb73acbdddac052c93025aa1f82942edabb7deb82a1",
			SizeBytes: 121,
		},
		ExpireAt:    timestamppb.Now(),
		LastUpdated: timestamppb.Now(),
	}, buffer.UserProvided)
	backend.EXPECT().Get(ctx, refDigest).Return(buf)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	baseFetcher := fetch.NewErrorFetcher(&protostatus.Status{
		Code:    5,
		Message: "Not found",
	})
	cacheFetcher := fetch.NewCachingFetcher(baseFetcher, assetStore)

	_, err = cacheFetcher.FetchBlob(ctx, request)

	errAsStatus := status.Convert(err)
	require.Contains(t, errAsStatus.Message(), "Not found")
	require.Contains(t, errAsStatus.Message(), "Asset expired at")
	require.Equal(t, errAsStatus.Code(), codes.NotFound)
}

func TestCachingFetcherOldestContentAccepted(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := digest.NewInstanceName("bar")
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
	require.NoError(t, err)

	uri := "https://example.com/exampleblob.zip"
	request := &remoteasset.FetchBlobRequest{
		InstanceName:          "bar",
		Uris:                  []string{uri},
		OldestContentAccepted: timestamppb.Now(),
	}
	_, refDigest, err := storage.ProtoSerialise(storage.NewAssetReference([]string{uri}, []*remoteasset.Qualifier{}), digestFunction)
	require.NoError(t, err)

	backend := mock.NewMockBlobAccess(ctrl)
	ts := timestamppb.New(time.Unix(1, 1))
	buf := buffer.NewProtoBufferFromProto(&asset.Asset{
		Digest: &remoteexecution.Digest{
			Hash:      "ad84ffc44bab3f84fc3396b4678c1fd39770fa373c3f14eedc5d60e648067960",
			SizeBytes: 234,
		},
		LastUpdated: ts,
		Type:        asset.Asset_BLOB,
	}, buffer.UserProvided)
	backend.EXPECT().Get(ctx, refDigest).Return(buf)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	baseFetcher := fetch.NewErrorFetcher(&protostatus.Status{
		Code:    5,
		Message: "Not found",
	})
	cacheFetcher := fetch.NewCachingFetcher(baseFetcher, assetStore)

	_, err = cacheFetcher.FetchBlob(ctx, request)
	errAsStatus := status.Convert(err)
	require.Contains(t, errAsStatus.Message(), "Not found")
	require.Contains(t, errAsStatus.Message(), "Asset older than")
	require.Equal(t, errAsStatus.Code(), codes.NotFound)
}

// TestFetchBlobCachingChecksumSriValidation exercises the self-healing
// behavior added to the cache-hit path: when a request supplies a
// checksum.sri qualifier and the AC entry's cached digest does NOT
// match the expected hash, the entry must be treated as a miss so the
// wrapped fetcher re-downloads and re-stores the correct content.
//
// The expected hex digest for the test below is precomputed from a
// real checksum.sri value:
//   sha256-GF+NsyJx/iX1Yab8k4suJkMG7DBO2lGAB9F2SCY4GWk=
// base64-decodes to bytes whose hex form is:
//   185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969
// (this is SHA256("Hello") — useful for any future end-to-end tests
// that want to round-trip through the http fetcher).
func TestFetchBlobCachingChecksumSriValidation(t *testing.T) {
	const (
		expectedHashHex = "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969"
		expectedSizeB   = 5
		checksumSri     = "sha256-GF+NsyJx/iX1Yab8k4suJkMG7DBO2lGAB9F2SCY4GWk="
		staleHashHex    = "7dadb03bc91583ce16597f30270a7d0f79e0f1227eb166b851447f946450d1bc"
		staleSizeB      = 517363235
	)

	uri := "https://artifactory.example.com/LLVM-20.1.1-Linux-X64.tar.zst"
	correctDigest := &remoteexecution.Digest{Hash: expectedHashHex, SizeBytes: expectedSizeB}
	staleDigest := &remoteexecution.Digest{Hash: staleHashHex, SizeBytes: staleSizeB}

	t.Run("CachedDigestMatchesChecksumSri_ReturnsCached", func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchBlobRequest{
			InstanceName: "",
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{Name: "checksum.sri", Value: checksumSri},
			},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, req.Qualifiers),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		// AC hit with a cached digest that matches checksum.sri.
		// No wrapped-fetcher call should happen.
		backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewBlobAsset(correctDigest, nil), buffer.UserProvided))

		response, err := cachingFetcher.FetchBlob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.BlobDigest, correctDigest))
	})

	t.Run("CachedDigestMismatchesChecksumSri_FallsThroughAndReheals", func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchBlobRequest{
			InstanceName: "",
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{Name: "checksum.sri", Value: checksumSri},
			},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, req.Qualifiers),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		// AC hit returns a stale (poisoned) digest.
		staleGetCall := backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewBlobAsset(staleDigest, nil), buffer.UserProvided))

		// Wrapped fetcher MUST be invoked with the same request — the
		// staleness check should cause the cache-hit path to fall through.
		freshFetchCall := mockFetcher.EXPECT().FetchBlob(ctx, req).Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Success!").Proto(),
			Uri:        uri,
			BlobDigest: correctDigest,
			Qualifiers: req.Qualifiers,
		}, nil).After(staleGetCall)

		// The fresh response MUST be re-stored under the same AC key,
		// overwriting the poisoned entry. This is the self-healing step.
		backend.EXPECT().Put(ctx, refDigest, gomock.Any()).DoAndReturn(
			func(_ context.Context, _ bb_digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&asset.Asset{}, 1000)
				require.NoError(t, err)
				a := m.(*asset.Asset)
				require.True(t, proto.Equal(a.Digest, correctDigest),
					"re-stored entry should contain the correct (fresh) digest")
				return nil
			}).After(freshFetchCall)

		response, err := cachingFetcher.FetchBlob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.BlobDigest, correctDigest),
			"client must receive the corrected digest, not the stale one")
	})

	t.Run("NoChecksumSri_CachedDigestReturnedAsIs", func(t *testing.T) {
		// Back-compat: requests without checksum.sri continue to
		// trust the cached digest. This covers older Bazel clients
		// and repository rules that don't pass a sha256.
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchBlobRequest{
			InstanceName: "",
			Uris:         []string{uri},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, []*remoteasset.Qualifier{}),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		// AC hit with what happens to be the "stale" digest from the
		// scenario above. Without checksum.sri, we have no basis to
		// reject it and must trust the cache.
		backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewBlobAsset(staleDigest, nil), buffer.UserProvided))

		response, err := cachingFetcher.FetchBlob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.BlobDigest, staleDigest))
	})

	t.Run("StaleThenFresh_SubsequentRequestHitsRehealedEntry", func(t *testing.T) {
		// End-to-end self-healing: first request finds stale → falls
		// through → re-stores correct. Second request with same
		// checksum.sri hits the cache and gets the correct digest
		// without a wrapped-fetcher call (proves the rewrite stuck).
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchBlobRequest{
			InstanceName: "",
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{Name: "checksum.sri", Value: checksumSri},
			},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, req.Qualifiers),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		// 1st request: AC has the stale entry → mismatch → fall through.
		staleGet := backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewBlobAsset(staleDigest, nil), buffer.UserProvided))
		freshFetch := mockFetcher.EXPECT().FetchBlob(ctx, req).Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Success!").Proto(),
			Uri:        uri,
			BlobDigest: correctDigest,
			Qualifiers: req.Qualifiers,
		}, nil).After(staleGet)
		reStorePut := backend.EXPECT().Put(ctx, refDigest, gomock.Any()).Return(nil).After(freshFetch)

		// 2nd request: AC now returns the correct entry → match → no
		// wrapped-fetcher call. This is the proof that healing stuck.
		backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewBlobAsset(correctDigest, nil), buffer.UserProvided),
		).After(reStorePut)

		resp1, err := cachingFetcher.FetchBlob(ctx, req)
		require.NoError(t, err)
		require.True(t, proto.Equal(resp1.BlobDigest, correctDigest))

		resp2, err := cachingFetcher.FetchBlob(ctx, req)
		require.NoError(t, err)
		require.True(t, proto.Equal(resp2.BlobDigest, correctDigest))
	})
}

// TestFetchDirectoryCachingChecksumSriValidation mirrors the FetchBlob
// validation for FetchDirectory. Directory fetches don't typically carry
// checksum.sri today but keeping the two paths consistent prevents
// drift.
func TestFetchDirectoryCachingChecksumSriValidation(t *testing.T) {
	const (
		expectedHashHex = "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969"
		checksumSri     = "sha256-GF+NsyJx/iX1Yab8k4suJkMG7DBO2lGAB9F2SCY4GWk="
		staleHashHex    = "7dadb03bc91583ce16597f30270a7d0f79e0f1227eb166b851447f946450d1bc"
	)

	uri := "https://example.com/release-tree.tar.gz"
	correctDigest := &remoteexecution.Digest{Hash: expectedHashHex, SizeBytes: 5}
	staleDigest := &remoteexecution.Digest{Hash: staleHashHex, SizeBytes: 999}

	t.Run("CachedDigestMismatchesChecksumSri_FallsThroughAndReheals", func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchDirectoryRequest{
			InstanceName: "",
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{Name: "checksum.sri", Value: checksumSri},
			},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, req.Qualifiers),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		staleGet := backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewDirectoryAsset(staleDigest, nil), buffer.UserProvided))
		freshFetch := mockFetcher.EXPECT().FetchDirectory(ctx, req).Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Success!").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: correctDigest,
			Qualifiers:          req.Qualifiers,
		}, nil).After(staleGet)
		backend.EXPECT().Put(ctx, refDigest, gomock.Any()).DoAndReturn(
			func(_ context.Context, _ bb_digest.Digest, b buffer.Buffer) error {
				m, err := b.ToProto(&asset.Asset{}, 1000)
				require.NoError(t, err)
				a := m.(*asset.Asset)
				require.True(t, proto.Equal(a.Digest, correctDigest))
				require.Equal(t, asset.Asset_DIRECTORY, a.Type)
				return nil
			}).After(freshFetch)

		response, err := cachingFetcher.FetchDirectory(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.RootDirectoryDigest, correctDigest))
	})

	t.Run("CachedDigestMatchesChecksumSri_ReturnsCached", func(t *testing.T) {
		ctrl, ctx := gomock.WithContext(context.Background(), t)

		instanceName, err := bb_digest.NewInstanceName("")
		require.NoError(t, err)
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256, 0)
		require.NoError(t, err)

		req := &remoteasset.FetchDirectoryRequest{
			InstanceName: "",
			Uris:         []string{uri},
			Qualifiers: []*remoteasset.Qualifier{
				{Name: "checksum.sri", Value: checksumSri},
			},
		}
		_, refDigest, err := storage.ProtoSerialise(
			storage.NewAssetReference([]string{uri}, req.Qualifiers),
			digestFunction)
		require.NoError(t, err)

		backend := mock.NewMockBlobAccess(ctrl)
		assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
		mockFetcher := mock.NewMockFetcher(ctrl)
		cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

		backend.EXPECT().Get(ctx, refDigest).Return(
			buffer.NewProtoBufferFromProto(storage.NewDirectoryAsset(correctDigest, nil), buffer.UserProvided))

		response, err := cachingFetcher.FetchDirectory(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int32(codes.OK), response.Status.Code)
		require.True(t, proto.Equal(response.RootDirectoryDigest, correctDigest))
	})
}

func TestFetchBlobVolatileQualifiersIgnored(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	uri := "https://example.com/blob.tar.gz"

	req1 := &remoteasset.FetchBlobRequest{
		InstanceName: "",
		Uris:         []string{uri},
		Qualifiers: []*remoteasset.Qualifier{
			{Name: "checksum.sri", Value: "sha256-aaa"},
			{Name: "http_header_url:0:Authorization", Value: "Bearer first"},
			{Name: "bazel.auth_headers", Value: "token‑one"},
		},
	}

	// 2nd request differs only in auth headers.
	req2 := proto.Clone(req1).(*remoteasset.FetchBlobRequest)
	req2.Qualifiers[1].Value = "Bearer second"
	req2.Qualifiers[2].Value = "token‑two"

	// 3rd request differs in non-auth headers and should be a cache miss from the first.
	req3 := proto.Clone(req1).(*remoteasset.FetchBlobRequest)
	req3.Qualifiers[0].Value = "Windows"

	blobDigest := &remoteexecution.Digest{
		Hash:      "1111111111111111111111111111111111111111111111111111111111111111",
		SizeBytes: 42,
	}

	backend := mock.NewMockBlobAccess(ctrl)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	mockFetcher := mock.NewMockFetcher(ctrl)
	cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

	// 1st fetch is a cache miss, and we'll record the digest used.
	var firstDigest bb_digest.Digest
	getMiss := backend.
		EXPECT().
		Get(ctx, gomock.Any()).
		Do(func(_ context.Context, d bb_digest.Digest) {
			firstDigest = d
		}).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "miss")))
	mockFetcher.
		EXPECT().
		FetchBlob(ctx, req1).
		After(getMiss).
		Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "fetched").Proto(),
			Uri:        uri,
			BlobDigest: blobDigest,
			Qualifiers: req1.Qualifiers,
		}, nil)
	backend.
		EXPECT().
		Put(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, d bb_digest.Digest, b buffer.Buffer) error {
			require.Equal(t, firstDigest, d)
			return nil
		})

	_, err := cachingFetcher.FetchBlob(ctx, req1)
	require.NoError(t, err)

	// 2nd fetch should be a cache hit, despite the auth qualifiers being different.
	backend.
		EXPECT().
		Get(ctx, firstDigest).
		Return(buffer.NewProtoBufferFromProto(storage.NewBlobAsset(blobDigest, nil), buffer.UserProvided))

	_, err = cachingFetcher.FetchBlob(ctx, req2)
	require.NoError(t, err)

	// 3rd fetch should be a cache miss since non-auth qualifiers differ.
	var thirdDigest bb_digest.Digest
	getMiss = backend.
		EXPECT().
		Get(ctx, gomock.Any()).
		Do(func(_ context.Context, d bb_digest.Digest) {
			require.NotEqual(t, firstDigest, d)
			thirdDigest = d
		}).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "miss")))
	mockFetcher.
		EXPECT().
		FetchBlob(ctx, req3).
		After(getMiss).
		Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "fetched").Proto(),
			Uri:        uri,
			BlobDigest: blobDigest,
			Qualifiers: req3.Qualifiers,
		}, nil)
	backend.
		EXPECT().
		Put(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, d bb_digest.Digest, b buffer.Buffer) error {
			require.Equal(t, thirdDigest, d)
			return nil
		})
	_, err = cachingFetcher.FetchBlob(ctx, req3)
	require.NoError(t, err)
}

func TestFetchDirectoryVolatileQualifiersIgnored(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	uri := "https://example.com/dir.zip"

	req1 := &remoteasset.FetchDirectoryRequest{
		InstanceName: "",
		Uris:         []string{uri},
		Qualifiers: []*remoteasset.Qualifier{
			{Name: "checksum.sri", Value: "sha256-aaa"},
			{Name: "http_header_url:0:Authorization", Value: "application/zip"},
			{Name: "bazel.auth_headers", Value: "token‑A"},
		},
	}

	// 2nd request differs only in auth headers.
	req2 := proto.Clone(req1).(*remoteasset.FetchDirectoryRequest)
	req2.Qualifiers[1].Value = "Bearer second"
	req2.Qualifiers[2].Value = "token‑two"

	// 3rd request differs in non-auth headers and should be a cache miss from the first.
	req3 := proto.Clone(req1).(*remoteasset.FetchDirectoryRequest)
	req3.Qualifiers[0].Value = "Windows"

	dirDigest := &remoteexecution.Digest{
		Hash:      "2222222222222222222222222222222222222222222222222222222222222222",
		SizeBytes: 99,
	}

	backend := mock.NewMockBlobAccess(ctrl)
	assetStore := storage.NewBlobAccessAssetStore(backend, 16*1024*1024)
	mockFetcher := mock.NewMockFetcher(ctrl)
	cachingFetcher := fetch.NewCachingFetcher(mockFetcher, assetStore)

	// 1st fetch is a cache miss, and we'll record the digest used.
	var firstDigest bb_digest.Digest
	getMiss := backend.
		EXPECT().
		Get(ctx, gomock.Any()).
		Do(func(_ context.Context, d bb_digest.Digest) {
			firstDigest = d
		}).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "miss")))
	mockFetcher.
		EXPECT().
		FetchDirectory(ctx, req1).
		After(getMiss).
		Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "fetched").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: dirDigest,
			Qualifiers:          req1.Qualifiers,
		}, nil)
	backend.
		EXPECT().
		Put(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, d bb_digest.Digest, b buffer.Buffer) error {
			require.Equal(t, firstDigest, d)
			return nil
		})

	_, err := cachingFetcher.FetchDirectory(ctx, req1)
	require.NoError(t, err)

	// 2nd fetch should be a cache hit, despite the auth qualifiers being different.
	backend.
		EXPECT().
		Get(ctx, firstDigest).
		Return(buffer.NewProtoBufferFromProto(storage.NewBlobAsset(dirDigest, nil), buffer.UserProvided))

	_, err = cachingFetcher.FetchDirectory(ctx, req2)
	require.NoError(t, err)

	// 3rd fetch should be a cache miss since non-auth qualifiers differ.
	var thirdDigest bb_digest.Digest
	getMiss = backend.
		EXPECT().
		Get(ctx, gomock.Any()).
		Do(func(_ context.Context, d bb_digest.Digest) {
			require.NotEqual(t, firstDigest, d)
			thirdDigest = d
		}).
		Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "miss")))
	mockFetcher.
		EXPECT().
		FetchDirectory(ctx, req3).
		After(getMiss).
		Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "fetched").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: dirDigest,
			Qualifiers:          req3.Qualifiers,
		}, nil)
	backend.
		EXPECT().
		Put(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, d bb_digest.Digest, b buffer.Buffer) error {
			require.Equal(t, thirdDigest, d)
			return nil
		})
	_, err = cachingFetcher.FetchDirectory(ctx, req3)
	require.NoError(t, err)
}
