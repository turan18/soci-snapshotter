package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/awslabs/soci-snapshotter/config"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/log"
	rhttp "github.com/hashicorp/go-retryablehttp"
)

type AuthClient struct {
	retryableClient *rhttp.Client
	authorizer      docker.Authorizer
	creds           func(string) (string, string, error)
	headers         http.Header
}

type AuthClientOpt func(*AuthClient)

func WithHeader(headers http.Header) AuthClientOpt {
	return func(ac *AuthClient) {
		ac.headers = headers
	}
}

func WithCredentialProvider(creds func(string) (string, string, error)) AuthClientOpt {
	return func(ac *AuthClient) {
		ac.creds = creds
	}
}

// NewStandardAuthClient creates returns an `*http.Client` that is capable of authenticating with registry hosts.
func NewStandardAuthClient(httpConfig config.RetryableHTTPClientConfig, opts ...AuthClientOpt) *http.Client {
	standardClient := http.DefaultClient

	retryClient := newRetryableClient(httpConfig)
	authClient := &AuthClient{
		retryableClient: retryClient,
	}
	for _, opt := range opts {
		opt(authClient)
	}
	standardClient.Transport = authClient
	authClient.authorizer = docker.NewDockerAuthorizer(docker.WithAuthClient(retryClient.StandardClient()), docker.WithAuthCreds(authClient.creds))
	return standardClient
}

func (ac *AuthClient) RoundTrip(req *http.Request) (*http.Response, error) {
	reqContext := req.Context()

	// Add global headers to request
	for k, _ := range ac.headers {
		req.Header.Set(k, ac.headers.Get(k))
	}
	if err := ac.authorizer.Authorize(reqContext, req); err != nil {
		return nil, err
	}

	// Send the request with the inner retryable client
	retryableRequest, err := rhttp.FromRequest(req)
	if err != nil {
		return nil, err
	}

	resp, err := ac.retryableClient.Do(retryableRequest)
	if err != nil {
		return nil, err
	}

	if shouldAuthenticate(resp) {
		log.G(reqContext).Infof("Received status code: %v. Refreshing creds...", resp.Status)
		if err := ac.authorizer.AddResponses(reqContext, []*http.Response{resp, resp}); err != nil {
			return nil, err
		}
		if err := ac.authorizer.AddResponses(reqContext, []*http.Response{resp}); err != nil {
			return nil, err
		}
		authReq := req.Clone(reqContext)
		Drain(resp.Body)
		return ac.RoundTrip(authReq)
	}

	return resp, nil
}

// func (ac *AuthClient) authorizeRequest(ctx context.Context, oldReq *rhttp.Request, resp *http.Response) (*http.Request, error) {

// 	// Prepare authorization for the target host using docker.Authorizer.
// 	// The docker authorizer only refreshes OAuth tokens after two
// 	// successive 401 errors for the same URL. Rather than issue the same
// 	// request multiple times to tickle the token-refreshing logic, just
// 	// provide the same response twice to trick it into refreshing the
// 	// cached OAuth token. Call AddResponses() twice, first to invalidate
// 	// the existing token (with two responses), second to fetch a new one
// 	// (with one response).
// 	// TODO: fix after one of these two PRs are merged and available:
// 	//     https://github.com/containerd/containerd/pull/8735
// 	//     https://github.com/containerd/containerd/pull/8388

// 	if err := ac.authorizer.Authorize(ctx, authReq); err != nil {
// 		return nil, err
// 	}
// 	if err := ac.authorizer.Authorize(ctx, authReq); err != nil {
// 		return nil, err
// 	}
// 	// log.G(ctx).Infof("HERE IS THE AUTH REQUEST: %+v", authReq)
// 	return authReq, nil
// }

// Clone creates a copy of the `AuthClient` with a new inner retryable client based off the
// provided `httpConfig`. It returns a standard `*http.Client`.
func (ac *AuthClient) Clone(httpConfig config.RetryableHTTPClientConfig) *http.Client {
	// The `authorizer` and `headers` are passed by reference, so the clone
	// will share both with the base instance.
	clone := &AuthClient{
		retryableClient: newRetryableClient(httpConfig),
		authorizer:      ac.authorizer,
		headers:         ac.headers,
	}
	return &http.Client{Transport: clone}

}

// shouldAuthenticate takes a HTTP response and determines whether or not
// it warrants authentication.
func shouldAuthenticate(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return true
	case http.StatusForbidden:

		/*
			Although in most cases 403 responses represent authorization issues that generally
			cannot be resolved by re-authentication, some registries like ECR, will return a 403 on
			credential expiration. (ref https://docs.aws.amazon.com/AmazonECR/latest/userguide/common-errors-docker.html#error-403)
			In the case of ECR, the response body is structured according to the error format defined in the
			Docker v2 API spec. (ref https://distribution.github.io/distribution/spec/api/#errors).
			We will attempt to decode the response body as a `docker.Errors`. If it can be decoded,
			we will ensure that the `Message` represents token expiration.
		*/

		// Since we drain the response body, we will copy it to a
		// buffer and re-assign it so that callers can still read
		// from it.
		body, err := io.ReadAll(resp.Body)
		defer func() {
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(body))
		}()

		if err != nil {
			return false
		}

		var errs docker.Errors
		if err = json.Unmarshal(body, &errs); err != nil {
			return false
		}
		for _, e := range errs {
			if err, ok := e.(docker.Error); ok {
				if err.Message == ECRTokenExpiredResponse {
					return true
				}
			}
		}

	default:
	}

	return false
}
