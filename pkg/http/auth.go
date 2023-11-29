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
	"context"
	"fmt"
	"net/http"

	rhttp "github.com/hashicorp/go-retryablehttp"

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
		return roundTrip(req.Clone(NewContextWithScope(ctx)))
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
