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
	"io"
	"net/http"
	"testing"

	"github.com/containerd/containerd/remotes/docker"
)

func TestAuthentication(t *testing.T) {

	ecrForbiddenResponse, _ := docker.Errors([]error{docker.ErrorCodeDenied.WithMessage(ECRTokenExpiredResponseMessage)}).MarshalJSON()
	normalForbiddenResponse, _ := docker.Errors([]error{docker.ErrorCodeDenied}).MarshalJSON()
	unauthorizedResponse, _ := docker.Errors([]error{docker.ErrorCodeUnauthorized}).MarshalJSON()

	testCases := []struct {
		name        string
		performAuth bool
		response    *http.Response
	}{
		{
			name:        "Authenticate on 403 with token expiry.",
			performAuth: true,
			response: &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(bytes.NewReader(ecrForbiddenResponse)),
			},
		},
		{
			name:        "Do not authenticate on 403 without token expiry.",
			performAuth: false,
			response: &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(bytes.NewReader(normalForbiddenResponse)),
			},
		},
		{
			name:        "Authenticate on 401.",
			performAuth: true,
			response: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(bytes.NewReader(unauthorizedResponse)),
			},
		},
		{
			name:        "Do not authenticate on 200.",
			performAuth: false,
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("data"))),
			},
		},
	}
	for _, tc := range testCases {
		shouldPerformAuthentication := ShouldAuthenticate(tc.response)
		if tc.performAuth != shouldPerformAuthentication {
			t.Fatalf("failed test case: %s: expected auth: %v; got auth: %v",
				tc.name, tc.performAuth, shouldPerformAuthentication)
		}
	}
}
