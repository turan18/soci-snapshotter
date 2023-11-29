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
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/awslabs/soci-snapshotter/config"
	logutil "github.com/awslabs/soci-snapshotter/util/http/log"
	"github.com/containerd/log"
	rhttp "github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
)

// newRetryableClient creates a go http.Client which will automatically
// retry on non-fatal errors
func newRetryableClient(config config.RetryableHTTPClientConfig) *rhttp.Client {
	rhttpClient := rhttp.NewClient()
	// Don't log every request
	rhttpClient.Logger = nil

	// set retry config
	rhttpClient.RetryMax = config.MaxRetries
	rhttpClient.RetryWaitMin = time.Duration(config.MinWaitMsec) * time.Millisecond
	rhttpClient.RetryWaitMax = time.Duration(config.MaxWaitMsec) * time.Millisecond
	rhttpClient.Backoff = backoffStrategy
	rhttpClient.CheckRetry = retryStrategy
	rhttpClient.ErrorHandler = handleHTTPError

	// set timeouts
	rhttpClient.HTTPClient.Timeout = time.Duration(config.RequestTimeoutMsec) * time.Millisecond
	innerTransport := rhttpClient.HTTPClient.Transport
	if t, ok := innerTransport.(*http.Transport); ok {
		t.DialContext = (&net.Dialer{
			Timeout: time.Duration(config.DialTimeoutMsec) * time.Millisecond,
		}).DialContext
		t.ResponseHeaderTimeout = time.Duration(config.ResponseHeaderTimeoutMsec) * time.Millisecond
	}

	return rhttpClient
}

// jitter returns a number in the range duration to duration+(duration/divisor)-1, inclusive
func jitter(duration time.Duration, divisor int64) time.Duration {
	return time.Duration(rand.Int63n(int64(duration)/divisor) + int64(duration))
}

// backoffStrategy extends retryablehttp's DefaultBackoff to add a random jitter to avoid
// overwhelming the repository when it comes back online
// DefaultBackoff either tries to parse the 'Retry-After' header of the response; or, it uses an
// exponential backoff 2 ^ numAttempts, limited by max
func backoffStrategy(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	delayTime := rhttp.DefaultBackoff(min, max, attemptNum, resp)
	return jitter(delayTime, 8)
}

// retryStrategy extends retryablehttp's DefaultRetryPolicy to log the error and response when retrying
// DefaultRetryPolicy retries whenever err is non-nil (except for some url errors) or if returned
// status code is 429 or 5xx (except 501)
func retryStrategy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	retry, err2 := rhttp.DefaultRetryPolicy(ctx, resp, err)
	if retry {
		log.G(ctx).WithFields(logrus.Fields{
			"error":    logutil.RedactHTTPQueryValuesFromError(err),
			"response": resp,
		}).Debugf("retrying request")
	}
	return retry, logutil.RedactHTTPQueryValuesFromError(err2)
}

// handleHTTPError implements retryablehttp client's ErrorHandler to ensure returned errors
// have HTTP query values redacted to prevent leaking sensitive information like encoded credentials or tokens.
func handleHTTPError(resp *http.Response, err error, attempts int) (*http.Response, error) {
	var (
		method = "unknown"
		url    = "unknown"
	)

	if resp != nil {
		Drain(resp.Body)

		if resp.Request != nil {

			method = resp.Request.Method

			if resp.Request.URL != nil {
				logutil.RedactHTTPQueryValuesFromURL(resp.Request.URL)
				url = resp.Request.URL.Redacted()
			}
		}
	}

	if err == nil {
		return nil, fmt.Errorf("%s \"%s\": giving up request after %d attempt(s)", method, url, attempts)
	}

	err = logutil.RedactHTTPQueryValuesFromError(err)
	return nil, fmt.Errorf("%s \"%s\": giving up request after %d attempt(s): %w", method, url, attempts, err)
}

// Drain tries to read and close the response body so the connection can be reused.
// See https://pkg.go.dev/net/http#Response for more information. Since it consumes
// the response body, this should only be used when the response body is no longer
// needed.
func Drain(body io.ReadCloser) {
	defer body.Close()

	// We want to consume response bodies to maintain HTTP connections,
	// but also want to limit the size read. 4KiB is arbitrary but reasonable.
	const responseReadLimit = int64(4096)
	_, _ = io.Copy(io.Discard, io.LimitReader(body, responseReadLimit))
}
