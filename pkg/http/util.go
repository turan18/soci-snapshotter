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
	"net/http"
	"strings"

	"github.com/containerd/containerd/remotes/docker"
)

// Redirect sends a GET request to a given endpoint with a given http.Client and
// returns the final URL in a redirect chain.
func Redirect(ctx context.Context, blobURL string, client *http.Client) (string, error) {
	if client.CheckRedirect != nil {
		return "", fmt.Errorf("client cannot contain a redirect policy")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
	if err != nil {
		return "", err
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrRequestFailed, err)
	}
	defer Drain(res.Body)

	if res.StatusCode/100 != 2 {
		return "", fmt.Errorf("%w on redirect %v", ErrUnexpectedStatusCode, res.StatusCode)
	}
	return res.Request.URL.String(), nil
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

// NewContextWithScope returns a new context that contains the
// registry auth scope of original context.
func NewContextWithScope(origCtx context.Context) context.Context {
	scope := docker.GetTokenScopes(origCtx, []string{})
	return docker.WithScope(context.Background(), strings.Join(scope, ""))
}
