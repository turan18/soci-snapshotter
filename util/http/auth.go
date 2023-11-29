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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	rhttp "github.com/hashicorp/go-retryablehttp"

	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/log"
)

// AuthHandler defines an interface for handling challenge-response
// based HTTP authentication.
//
// See: https://datatracker.ietf.org/doc/html/rfc9110#section-11
type AuthHandler interface {
	// HandleChallenge is responsible for parsing the challenge defined
	// by the origin server and preparing a valid response/answer.
	HandleChallenge(context.Context, *http.Response) error
	// AuthorizeRequest is responsible for authorizing the request to be
	// sent to the origin server.
	AuthorizeRequest(context.Context, *http.Request) (*http.Request, error)
}

// AuthPolicy defines an authentication policy. It takes a response
// and determines whether or not it warrants authentication.
type AuthPolicy func(*http.Response) bool

// DefaultAuthPolicy defines the default AuthPolicy, where by only a "401
// Unauthorized" constitutes authentication.
var DefaultAuthPolicy = func(resp *http.Response) bool {
	return resp.StatusCode == http.StatusUnauthorized
}

// AuthClient provides a HTTP client that is capable of authenticating
// with origin servers. It embeds an AuthHandler type that is responsible
// for preparing valid responses/answers to challenges as well authenticating
// requests. It wraps an inner retryable client, that is uses to send requests.
//
// Note: The AuthClient does not directly provide a mechanism for caching
// credentials/tokens. Ideally, this should be handled by the underlying
// AuthHandler.
type AuthClient struct {
	client  *rhttp.Client
	handler AuthHandler
	policy  AuthPolicy
	headers http.Header
}

type AuthClientOpt func(*AuthClient)

// WithHeader adds a http.Header to the AuthClient that will
// be attached to every request.
func WithHeader(headers http.Header) AuthClientOpt {
	return func(ac *AuthClient) {
		ac.headers = headers
	}
}

// WithAuthPolicy attaches an AuthPolicy to the AuthClient.
func WithAuthPolicy(policy AuthPolicy) AuthClientOpt {
	return func(ac *AuthClient) {
		ac.policy = policy
	}
}

// WithRetryableClient attaches a retryable client to the AuthClient.
func WithRetryableClient(client *rhttp.Client) AuthClientOpt {
	return func(ac *AuthClient) {
		ac.client = client
	}
}

// NewAuthClient creates a new AuthClient given an AuthHandler.
//
// An AuthHandler must be provided. If no retryable client is provided
// it will create a default retryable client. If no AuthPolicy is
// provided the DefaultAuthPolicy will be used.
func NewAuthClient(authHandler AuthHandler, opts ...AuthClientOpt) (*AuthClient, error) {
	if authHandler == nil {
		return nil, ErrMissingAuthHandler
	}
	ac := &AuthClient{
		handler: authHandler,
	}
	for _, opt := range opts {
		opt(ac)
	}
	if ac.client == nil {
		ac.client = rhttp.NewClient()
	}
	if ac.policy == nil {
		ac.policy = DefaultAuthPolicy
	}
	return ac, nil
}

// Do sends a request using the underlying retryable client. If no
// error is returned and the AuthPolicy deems that the response
// warrants authentication, it will invoke the AuthHandler to handle
// the challenge, re-authorize and re-send the request.
func (ac *AuthClient) Do(req *http.Request) (*http.Response, error) {
	if ac.client == nil {
		ac.client = rhttp.NewClient()
	}
	ctx := req.Context()
	roundTrip := func(req *http.Request) (*http.Response, error) {
		// Attach global headers to the request.
		for k := range ac.headers {
			req.Header.Set(k, ac.headers.Get(k))
		}
		authReq, err := ac.handler.AuthorizeRequest(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFailedToAuthorizeRequest, err)
		}
		// Convert the request to be a "retryable" request.
		rAuthReq, err := rhttp.FromRequest(authReq)
		if err != nil {
			return nil, err
		}
		resp, err := ac.client.Do(rAuthReq)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	resp, err := roundTrip(req)
	if err != nil {
		return nil, err
	}

	if ac.policy(resp) {
		log.G(ctx).Infof("Received status code: %v. Authorizing...", resp.Status)
		err = ac.handler.HandleChallenge(ctx, resp)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFailedToHandleChallenge, err)
		}
		Drain(resp.Body)
		return roundTrip(req.Clone(newContextWithScope(ctx)))
	}

	return resp, nil
}

// StandardClient returns a standard http.Client with the AuthClient embedded as its
// inner Transport.
//
// Consumers should use this when dealing with API's that strictly accept http.Client's.
func (ac *AuthClient) StandardClient() *http.Client {
	return &http.Client{Transport: ac}
}

// RoundTrip calls the AuthClient's underlying Do method. It exists
// so that the AuthClient can fulfill the http.RoundTripper interface,
// enabling it to be embedded inside a standard http.Client.
func (ac *AuthClient) RoundTrip(req *http.Request) (*http.Response, error) {
	return ac.Do(req)
}

// CloneWithNewClient returns a clone of the AuthClient with a new inner
// retryable client.
func (ac *AuthClient) CloneWithNewClient(client *rhttp.Client) *AuthClient {
	return &AuthClient{
		client:  client,
		policy:  ac.policy,
		handler: ac.handler,
		headers: ac.headers,
	}
}

// Client returns the inner retryable client.
func (ac *AuthClient) Client() *rhttp.Client {
	return ac.client
}

type dockerAuthHandler struct {
	authorizer docker.Authorizer
}

// NewDockerAuthHandler implements the AuthHandler interface, using
// a docker.Authorizer to handle authentication.
func NewDockerAuthHandler(authorizer docker.Authorizer) AuthHandler {
	return &dockerAuthHandler{
		authorizer: authorizer,
	}
}

// HandleChallenge calls the underlying docker.Authorizer's AddResponses method.
func (d *dockerAuthHandler) HandleChallenge(ctx context.Context, resp *http.Response) error {
	// Prepare authorization for the target host using docker.Authorizer.
	// The docker authorizer only refreshes OAuth tokens after two
	// successive 401 errors for the same URL. Rather than issue the same
	// request multiple times to tickle the token-refreshing logic, just
	// provide the same response twice to trick it into refreshing the
	// cached OAuth token. Call AddResponses() twice, first to invalidate
	// the existing token (with two responses), second to fetch a new one
	// (with one response).
	// TODO: fix after one of these two PRs are merged and available:
	//     https://github.com/containerd/containerd/pull/8735
	//     https://github.com/containerd/containerd/pull/8388
	if err := d.authorizer.AddResponses(ctx, []*http.Response{resp, resp}); err != nil {
		return err
	}
	return d.authorizer.AddResponses(ctx, []*http.Response{resp})

}

// AuthorizeRequest calls the underlying docker.Authorizer's Authorize method.
func (d *dockerAuthHandler) AuthorizeRequest(ctx context.Context, req *http.Request) (*http.Request, error) {
	err := d.authorizer.Authorize(ctx, req)
	return req, err
}

// ShouldAuthenticate takes a HTTP response from a registry and determines whether or not
// it warrants authentication.
func ShouldAuthenticate(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return true
	case http.StatusForbidden:

		/*
			Although in most cases 403 responses represent authorization issues that generally
			cannot be resolved by re-authentication, some registries like ECR, will return a 403 on
			credential expiration.
			See: https://docs.aws.amazon.com/AmazonECR/latest/userguide/common-errors-docker.html#error-403)

			In the case of ECR, the response body is structured according to the error format defined in the
			Docker v2 API spec. See: https://distribution.github.io/distribution/spec/api/#errors).
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
				if err.Message == ECRTokenExpiredResponseMessage {
					return true
				}
			}
		}
	case http.StatusBadRequest:
		/*
			S3 returns a 400 on token expiry with an XML encoded response body.
			See: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList

			We will decode the response body and ensure the `Code` represents token expiration.
			If it does, we will normalize the response status (eg: convert it to a standard 401 Unauthorized).
			The pre-signed S3 URL will need to be refreshed by the underlying blob fetcher.

		*/
		if resp.Header.Get("Content-Type") == "application/xml" {
			var s3Error struct {
				XMLName   xml.Name `xml:"Error"`
				Code      string   `xml:"Code"`
				Message   string   `xml:"Message"`
				Resource  string   `xml:"Resource"`
				RequestID string   `xml:"RequestId"`
			}
			body, err := io.ReadAll(resp.Body)
			defer func() {
				resp.Body.Close()
				resp.Body = io.NopCloser(bytes.NewReader(body))
			}()
			if err != nil {
				return false
			}
			if err = xml.Unmarshal(body, &s3Error); err != nil {
				return false
			}
			if s3Error.Code == S3TokenExpiredResponseCode {
				resp.Status = "401 Unauthorized"
				resp.StatusCode = http.StatusUnauthorized
				return false
			}
		}
	default:
	}

	return false
}

func newContextWithScope(ctx context.Context) context.Context {
	scope := docker.GetTokenScopes(ctx, []string{})
	return docker.WithScope(context.Background(), strings.Join(scope, ""))
}
