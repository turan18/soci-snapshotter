/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package remote

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/soci-snapshotter/cache"
	"github.com/awslabs/soci-snapshotter/config"
	commonmetrics "github.com/awslabs/soci-snapshotter/fs/metrics/common"
	shttp "github.com/awslabs/soci-snapshotter/util/http"
	logutil "github.com/awslabs/soci-snapshotter/util/http/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type fetcher interface {
	fetch(ctx context.Context, rs []region, retry bool) (multipartReadCloser, error)
	check() error
	genID(reg region) string
}
type Handler interface {
	Handle(ctx context.Context, desc ocispec.Descriptor) (fetcher Fetcher, size int64, err error)
}

type fetcherConfig struct {
	hosts        []docker.RegistryHost
	refspec      reference.Spec
	desc         ocispec.Descriptor
	fetchTimeout time.Duration
	maxRetries   int
	minWait      time.Duration
	maxWait      time.Duration
}

type Resolver struct {
	blobConfig config.BlobConfig
	handlers   map[string]Handler
}

func NewResolver(cfg config.BlobConfig, handlers map[string]Handler) *Resolver {
	return &Resolver{
		blobConfig: cfg,
		handlers:   handlers,
	}
}

func (r *Resolver) Resolve(ctx context.Context, hosts []docker.RegistryHost, refspec reference.Spec, desc ocispec.Descriptor, blobCache cache.BlobCache) (Blob, error) {

	var (
		validInterval = time.Duration(r.blobConfig.ValidInterval) * time.Second
		fetchTimeout  = time.Duration(r.blobConfig.FetchTimeoutSec) * time.Second
		minWait       = time.Duration(r.blobConfig.MinWaitMsec) * time.Millisecond
		maxWait       = time.Duration(r.blobConfig.MaxWaitMsec) * time.Millisecond
		maxRetries    = r.blobConfig.MaxRetries
	)

	f, size, err := r.resolveFetcher(ctx, &fetcherConfig{
		hosts:        hosts,
		refspec:      refspec,
		desc:         desc,
		fetchTimeout: fetchTimeout,
		maxRetries:   maxRetries,
		minWait:      minWait,
		maxWait:      maxWait,
	})
	if err != nil {
		return nil, err
	}
	return makeBlob(
			f,
			size,
			time.Now(),
			validInterval,
			r),
		nil
}

func (r *Resolver) resolveFetcher(ctx context.Context, fc *fetcherConfig) (f fetcher, size int64, err error) {
	var handlersErr error
	for name, p := range r.handlers {
		// TODO: allow to configure the selection of readers based on the hostname in refspec
		r, size, err := p.Handle(ctx, fc.desc)
		if err != nil {
			handlersErr = errors.Join(handlersErr, err)
			continue
		}
		log.G(ctx).WithField("handler name", name).WithField("ref", fc.refspec.String()).WithField("digest", fc.desc.Digest).
			Debugf("contents is provided by a handler")
		return &remoteFetcher{r}, size, nil
	}

	logger := log.G(ctx)
	if handlersErr != nil {
		logger = logger.WithError(handlersErr)
	}
	logger.WithField("ref", fc.refspec.String()).WithField("digest", fc.desc.Digest).Debugf("using default handler")

	hf, err := newHTTPFetcher(ctx, fc)
	if err != nil {
		return nil, 0, err
	}
	if fc.desc.Size == 0 {
		logger.WithField("ref", fc.refspec.String()).WithField("digest", fc.desc.Digest).
			Debugf("layer size not found in labels; making a request to remote to get size")

		fc.desc.Size, err = getLayerSize(ctx, hf)
		if err != nil {
			return nil, 0, fmt.Errorf("%s from %s: %w", ErrFailedToRetrieveLayerSize, hf.safeRealBlobURL, err)
		}
	}
	if r.blobConfig.ForceSingleRangeMode {
		hf.singleRangeMode()
	}
	return hf, fc.desc.Size, err
}

type httpFetcher struct {
	client          *http.Client
	scope           string
	safeRealBlobURL string
	realBlobURL     string
	baseBlobURL     string
	urlMu           sync.Mutex
	digest          digest.Digest
	singleRange     bool
	singleRangeMu   sync.Mutex
}

func newHTTPFetcher(ctx context.Context, fc *fetcherConfig) (*httpFetcher, error) {
	desc := fc.desc
	if desc.Digest.String() == "" {
		return nil, fmt.Errorf("missing digest; a digest is mandatory in layer descriptor")
	}
	digest := desc.Digest

	pullScope, err := docker.RepositoryScope(fc.refspec, false)
	if err != nil {
		return nil, err
	}

	// Try to create fetcher until succeeded
	createFetcherErr := errors.New("")
	for _, host := range fc.hosts {
		if host.Host == "" || strings.Contains(host.Host, "/") {
			createFetcherErr = fmt.Errorf("%w (host %q, ref:%q, digest:%q): %w",
				ErrInvalidHost, host.Host, fc.refspec, digest, createFetcherErr)
			// Try another
			continue
		}

		hostClient := host.Client
		tr := hostClient.Transport
		if authClient, ok := tr.(*shttp.AuthClient); ok {
			// Get the inner retryable client.
			retryClient := authClient.Client()
			retryClient.HTTPClient.Timeout = fc.fetchTimeout
			retryClient.RetryMax = fc.maxRetries
			retryClient.RetryWaitMin = fc.minWait
			retryClient.RetryWaitMax = fc.maxWait
		}

		ctx = docker.WithScope(ctx, pullScope)
		// Resolve redirection and get blob URL
		baseBlobURL := fmt.Sprintf("%s://%s/%s/blobs/%s",
			host.Scheme,
			path.Join(host.Host, host.Path),
			strings.TrimPrefix(fc.refspec.Locator, fc.refspec.Hostname()+"/"),
			digest)
		realURL, err := redirect(ctx, baseBlobURL, hostClient)
		if err != nil {
			createFetcherErr = fmt.Errorf("%w (host %q, ref:%q, digest:%q): %v: %w",
				ErrFailedToRedirect, host.Host, fc.refspec, digest, err, createFetcherErr)
			// Try another
			continue
		}

		// The backend URL may contain sensitive information like credentials
		// in it's query parameters. In this case, we redact this information
		// from the URL. We should always use the safe URL when logging or returning
		// errors.
		safeRealBlobURL, err := url.Parse(realURL)
		if err != nil {
			return nil, err
		}
		logutil.RedactHTTPQueryValuesFromURL(safeRealBlobURL)

		// Hit one destination
		return &httpFetcher{
			client:          hostClient,
			scope:           pullScope,
			baseBlobURL:     baseBlobURL,
			safeRealBlobURL: safeRealBlobURL.String(),
			realBlobURL:     realURL,
			digest:          digest,
		}, nil
	}

	return nil, fmt.Errorf("%w: %w", ErrUnableToCreateFetcher, createFetcherErr)
}

func (f *httpFetcher) fetch(ctx context.Context, rs []region, retry bool) (multipartReadCloser, error) {
	ctx = docker.WithScope(ctx, f.scope)
	if len(rs) == 0 {
		return nil, ErrNoRegion
	}

	singleRangeMode := f.isSingleRangeMode()

	// squash requesting regions for reducing the total size of request header
	// (servers generally have limits for the size of headers)
	// TODO: when our request has too many ranges, we need to divide it into
	//       multiple requests to avoid huge header.
	var s regionSet
	for _, reg := range rs {
		s.add(reg)
	}
	requests := s.rs
	if singleRangeMode {
		// Squash requests if the layer doesn't support multi range.
		requests = []region{superRegion(requests)}
	}

	// Request to the registry
	f.urlMu.Lock()
	url := f.realBlobURL
	f.urlMu.Unlock()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	var ranges string
	for _, reg := range requests {
		ranges += fmt.Sprintf("%d-%d,", reg.b, reg.e)
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%s", ranges[:len(ranges)-1]))
	req.Header.Add("Accept-Encoding", "identity")
	req.Close = false

	// Recording the roundtrip latency for remote registry GET operation.
	start := time.Now()
	res, err := f.client.Do(req)
	commonmetrics.MeasureLatencyInMilliseconds(commonmetrics.RemoteRegistryGet, f.digest, start)
	if err != nil {
		return nil, err
	}

	switch res.StatusCode {
	case http.StatusOK:
		// We are getting the whole blob in one part (= status 200)
		size, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCannotParseContentLength, err)
		}
		return newSinglePartReader(region{0, size - 1}, res.Body), nil
	case http.StatusPartialContent:
		mediaType, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid media type %q: %w", ErrCannotParseContentType, mediaType, err)
		}
		if strings.HasPrefix(mediaType, "multipart/") {
			// We are getting a set of regions as a multipart body.
			return newMultiPartReader(res.Body, params["boundary"]), nil
		}
		// We are getting single range
		reg, _, err := parseRange(res.Header.Get("Content-Range"))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCannotParseContentRange, err)
		}
		return newSinglePartReader(reg, res.Body), nil
	case http.StatusUnauthorized, http.StatusForbidden:
		// The underlying AuthClient should have already handled a 401 response.
		// This may indicate token expiry for the blob URL. Attempt a single URL
		// refresh.
		if retry {
			log.G(ctx).Infof("Received status code: %v. Refreshing URL and retrying...", res.Status)
			if err := f.refreshURL(ctx); err != nil {
				return nil, fmt.Errorf("%w: status %v: %w", ErrFailedToRefreshURL, res.Status, err)
			}
			return f.fetch(ctx, rs, false)
		}
	case http.StatusBadRequest:
		// gcr.io (https://storage.googleapis.com) returns 400 on multi-range request (2020 #81)
		if retry && !singleRangeMode {
			log.G(ctx).Infof("Received status code: %v. Setting single range mode and retrying...", res.Status)

			f.singleRangeMode()            // fallbacks to singe range request mode
			return f.fetch(ctx, rs, false) // retries with the single range mode
		}
	}
	return nil, fmt.Errorf("%w on fetch: %v", ErrUnexpectedStatusCode, res.Status)
}

func (f *httpFetcher) check() error {
	ctx := context.Background()
	f.urlMu.Lock()
	url := f.realBlobURL
	f.urlMu.Unlock()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("check failed: %w", err)
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("check failed: %w: %w", ErrRequestFailed, err)
	}
	defer shttp.Drain(res.Body)
	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusPartialContent {
		return nil
	} else if res.StatusCode == http.StatusForbidden {
		// Try to re-redirect this blob
		rCtx := context.Background()
		if err := f.refreshURL(rCtx); err == nil {
			return nil
		}
		return fmt.Errorf("%w: status %v", ErrFailedToRefreshURL, res.Status)
	}

	return fmt.Errorf("%w on check: %v", ErrUnexpectedStatusCode, res.StatusCode)
}

func (f *httpFetcher) refreshURL(ctx context.Context) error {
	newRealBlobURL, err := redirect(ctx, f.baseBlobURL, f.client)
	if err != nil {
		return err
	}
	f.urlMu.Lock()
	f.realBlobURL = newRealBlobURL
	f.urlMu.Unlock()
	return nil
}

func (f *httpFetcher) genID(reg region) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", f.baseBlobURL, reg.b, reg.e)))
	return fmt.Sprintf("%x", sum)
}

func (f *httpFetcher) singleRangeMode() {
	f.singleRangeMu.Lock()
	f.singleRange = true
	f.singleRangeMu.Unlock()
}

func (f *httpFetcher) isSingleRangeMode() bool {
	f.singleRangeMu.Lock()
	r := f.singleRange
	f.singleRangeMu.Unlock()
	return r
}

func redirect(ctx context.Context, blobURL string, client *http.Client) (string, error) {
	// We use GET request for redirect.
	// gcr.io returns 200 on HEAD without Location header (2020).
	// ghcr.io returns 200 on HEAD without Location header (2020).
	req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
	if err != nil {
		return "", err
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	// The underlying HTTP client will follow up to 10 redirects.
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrRequestFailed, err)
	}
	defer shttp.Drain(res.Body)

	if res.StatusCode/100 != 2 {
		return "", fmt.Errorf("%w on redirect %v", ErrUnexpectedStatusCode, res.StatusCode)
	}
	return res.Request.URL.String(), nil
}

func getLayerSize(ctx context.Context, hf *httpFetcher) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", hf.realBlobURL, nil)
	if err != nil {
		return 0, err
	}
	req.Close = false
	res, err := hf.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	}
	headStatusCode := res.StatusCode

	// Failed to do HEAD request. Fall back to GET.
	// ghcr.io (https://github-production-container-registry.s3.amazonaws.com) doesn't allow
	// HEAD request (2020).
	req, err = http.NewRequestWithContext(ctx, "GET", hf.realBlobURL, nil)
	if err != nil {
		return 0, err
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err = hf.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrRequestFailed, err)
	}
	defer shttp.Drain(res.Body)

	if res.StatusCode == http.StatusOK {
		return strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	} else if res.StatusCode == http.StatusPartialContent {
		_, size, err := parseRange(res.Header.Get("Content-Range"))
		return size, err
	}

	return 0, fmt.Errorf("failed to get size with code (HEAD=%v, GET=%v)",
		headStatusCode, res.StatusCode)
}

type multipartReadCloser interface {
	Next() (region, io.Reader, error)
	Close() error
}

func newSinglePartReader(reg region, rc io.ReadCloser) multipartReadCloser {
	return &singlepartReader{
		r:      rc,
		Closer: rc,
		reg:    reg,
	}
}

type singlepartReader struct {
	io.Closer
	r      io.Reader
	reg    region
	called bool
}

func (sr *singlepartReader) Next() (region, io.Reader, error) {
	if !sr.called {
		sr.called = true
		return sr.reg, sr.r, nil
	}
	return region{}, nil, io.EOF
}

func newMultiPartReader(rc io.ReadCloser, boundary string) multipartReadCloser {
	return &multipartReader{
		m:      multipart.NewReader(rc, boundary),
		Closer: rc,
	}
}

type multipartReader struct {
	io.Closer
	m *multipart.Reader
}

func (sr *multipartReader) Next() (region, io.Reader, error) {
	p, err := sr.m.NextPart()
	if err != nil {
		return region{}, nil, err
	}
	reg, _, err := parseRange(p.Header.Get("Content-Range"))
	if err != nil {
		return region{}, nil, fmt.Errorf("%w: %w", ErrCannotParseContentRange, err)
	}
	return reg, p, nil
}

func parseRange(header string) (region, int64, error) {
	submatches := contentRangeRegexp.FindStringSubmatch(header)
	if len(submatches) < 4 {
		return region{}, 0, fmt.Errorf("Content-Range %q doesn't have enough information", header)
	}
	begin, err := strconv.ParseInt(submatches[1], 10, 64)
	if err != nil {
		return region{}, 0, fmt.Errorf("failed to parse beginning offset %q: %w", submatches[1], err)
	}
	end, err := strconv.ParseInt(submatches[2], 10, 64)
	if err != nil {
		return region{}, 0, fmt.Errorf("failed to parse end offset %q: %w", submatches[2], err)
	}
	blobSize, err := strconv.ParseInt(submatches[3], 10, 64)
	if err != nil {
		return region{}, 0, fmt.Errorf("failed to parse blob size %q: %w", submatches[3], err)
	}

	return region{begin, end}, blobSize, nil
}

type Option func(*options)

type options struct {
	ctx       context.Context
	cacheOpts []cache.Option
}

func WithContext(ctx context.Context) Option {
	return func(opts *options) {
		opts.ctx = ctx
	}
}

func WithCacheOpts(cacheOpts ...cache.Option) Option {
	return func(opts *options) {
		opts.cacheOpts = cacheOpts
	}
}

type remoteFetcher struct {
	r Fetcher
}

func (r *remoteFetcher) fetch(ctx context.Context, rs []region, retry bool) (multipartReadCloser, error) {
	var s regionSet
	for _, reg := range rs {
		s.add(reg)
	}
	reg := superRegion(s.rs)
	rc, err := r.r.Fetch(ctx, reg.b, reg.size())
	if err != nil {
		return nil, err
	}
	return newSinglePartReader(reg, rc), nil
}

func (r *remoteFetcher) check() error {
	return r.r.Check()
}

func (r *remoteFetcher) genID(reg region) string {
	return r.r.GenID(reg.b, reg.size())
}

type Fetcher interface {
	Fetch(ctx context.Context, off int64, size int64) (io.ReadCloser, error)
	Check() error
	GenID(off int64, size int64) string
}
