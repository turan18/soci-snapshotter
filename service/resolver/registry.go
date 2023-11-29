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

package resolver

import (
	"sync"
	"time"

	"github.com/awslabs/soci-snapshotter/config"
	rhttp "github.com/hashicorp/go-retryablehttp"

	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
)

// Credential returns a set of credentials for a given image.
type Credential func(imgRefSpec reference.Spec) (string, string, error)

// RegistryHosts returns configurations for registry hosts that provide a given image.
type RegistryHosts func(imgRefSpec reference.Spec) ([]docker.RegistryHost, error)

// RegistryManager contains the configurations that outline how remote
// registry operations should behave. It contains a global retryable client
// that will be used for all registry requests.
type RegistryManager struct {
	// retryClient is the global retryable client
	retryClient *rhttp.Client
	// registryConfig is the per-host registry config
	registryConfig config.ResolverConfig
	// creds are the list of credential providers
	creds []Credential
	// registryHostMap is a map of image reference to registry configurations
	registryHostMap *sync.Map
}

// NewRegistryManager returns a new RegistryManager
func NewRegistryManager(httpConfig config.RetryableHTTPClientConfig, registryConfig config.ResolverConfig, credsFuncs []Credential) *RegistryManager {
	return &RegistryManager{
		retryClient:     newRetryableClientFromConfig(httpConfig),
		creds:           credsFuncs,
		registryConfig:  registryConfig,
		registryHostMap: &sync.Map{},
	}
}

// AsRegistryHosts returns a RegistryHosts type responsible for returning
// registry host configurations with respect to the configurations
// present in RegistryManager.
func (rm *RegistryManager) AsRegistryHosts() RegistryHosts {
	// TODO: Document reasoning for image-level `AuthClient`
	return func(imgRefSpec reference.Spec) ([]docker.RegistryHost, error) {
		// Check whether registry host configurations exist for this image ref
		// in the cache.
		if hostsConfigurations, ok := rm.registryHostMap.Load(imgRefSpec.String()); ok {
			return hostsConfigurations.([]docker.RegistryHost), nil
		}

		var registryHosts []docker.RegistryHost
		host := imgRefSpec.Hostname()
		// If mirrors exist for the host that provides this image, create new
		// `RegistryHost` configurations for them.
		if hostConfig, ok := rm.registryConfig.Host[host]; ok {
			for _, mirror := range hostConfig.Mirrors {
				scheme := "https"
				if localhost, _ := docker.MatchLocalhost(mirror.Host); localhost || mirror.Insecure {
					scheme = "http"
				}
				rc := rm.retryClient
				// If a RequestTimeoutSec is set, we will need to create a new retryable client.
				if mirror.RequestTimeoutSec != 0 {
					rc = CloneRetryableClient(rc)
					if mirror.RequestTimeoutSec < 0 {
						rc.HTTPClient.Timeout = 0
					} else {
						rc.HTTPClient.Timeout = time.Duration(mirror.RequestTimeoutSec) * time.Second
					}
					// Re-use the same transport so we can use a single
					// global connection pool.
					rc.HTTPClient.Transport = rm.retryClient.HTTPClient.Transport
				}
				mirrorImgRefSpec, err := newRefSpecWithHost(imgRefSpec, mirror.Host)
				if err != nil {
					return nil, err
				}
				ac, err := newAuthClient(rc, multiCredsFuncs(mirrorImgRefSpec, rm.creds...))
				if err != nil {
					return nil, err
				}
				registryHosts = append(registryHosts, docker.RegistryHost{
					Client:       ac.StandardClient(),
					Host:         mirror.Host,
					Scheme:       scheme,
					Path:         "/v2",
					Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
				})
			}
		}

		authClient, err := newAuthClient(rm.retryClient, multiCredsFuncs(imgRefSpec, rm.creds...))
		if err != nil {
			return nil, err
		}
		if host == "docker.io" {
			host = "registry-1.docker.io"
		}
		// Create a `RegistryHost` configuration for this host.
		registryHosts = append(registryHosts, docker.RegistryHost{
			Client:       authClient.StandardClient(),
			Host:         host,
			Scheme:       "https",
			Path:         "/v2",
			Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
		})

		// Cache `RegistryHost` configurations for all hosts that provide this image.
		rm.registryHostMap.Store(imgRefSpec.String(), registryHosts)

		return registryHosts, nil
	}
}

// multiCredsFuncs joins a list of credential functions into a single credential function.
func multiCredsFuncs(imgRefSpec reference.Spec, credsFuncs ...Credential) func(string) (string, string, error) {
	return func(_ string) (string, string, error) {
		for _, f := range credsFuncs {
			if username, secret, err := f(imgRefSpec); err != nil {
				return "", "", err
			} else if !(username == "" && secret == "") {
				return username, secret, nil
			}
		}
		return "", "", nil
	}
}
