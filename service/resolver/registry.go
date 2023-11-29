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
	"fmt"
	"net/http"
	"sync"

	"github.com/awslabs/soci-snapshotter/config"
	shttp "github.com/awslabs/soci-snapshotter/util/http"
	"github.com/awslabs/soci-snapshotter/version"
	"github.com/containerd/containerd/remotes/docker"
)

var UserAgent = fmt.Sprintf("soci-snapshotter/%s", version.Version)

// Credential returns a set of credentials for a given host.
type Credential func(string) (string, string, error)

// RegistryManager contains the configurations that outline how remote
// registry operations should behave. It contains a global http.Client
// that will be used for all registry requests.
type RegistryManager struct {
	client          *http.Client
	httpConfig      config.RetryableHTTPClientConfig
	registryConfig  config.ResolverConfig
	registryHostMap *sync.Map
}

// GlobalHeaders returns a global http.Header that should be attached
// to all requests.
func GlobalHeaders() http.Header {
	header := http.Header{}
	header.Set("User-Agent", UserAgent)
	return header
}

// NewRegistryManager returns a new RegistryManager
func NewRegistryManager(httpConfig config.RetryableHTTPClientConfig, registryConfig config.ResolverConfig, credsFuncs []Credential) (*RegistryManager, error) {
	registryManager := &RegistryManager{
		httpConfig:      httpConfig,
		registryConfig:  registryConfig,
		registryHostMap: &sync.Map{},
	}

	retryClient := shttp.NewRetryableClient(httpConfig)

	globalAuthorizer := docker.NewDockerAuthorizer(docker.WithAuthClient(retryClient.StandardClient()),
		docker.WithAuthCreds(multiCredsFuncs(credsFuncs...)), docker.WithAuthHeader(GlobalHeaders()))

	authClientOpts := []shttp.AuthClientOpt{shttp.WithRetryableClient(retryClient),
		shttp.WithAuthPolicy(shttp.ShouldAuthenticate), shttp.WithHeader(GlobalHeaders())}

	globalAuthClient, err := shttp.NewAuthClient(shttp.NewDockerAuthHandler(globalAuthorizer), authClientOpts...)
	if err != nil {
		return nil, err
	}
	registryManager.client = globalAuthClient.StandardClient()
	return registryManager, nil
}

// ConfigureRegistries returns a RegistryHosts type responsible for returning
// registry configurations for a specific host.
func (rm *RegistryManager) ConfigureRegistries() docker.RegistryHosts {
	return func(host string) ([]docker.RegistryHost, error) {
		// Check whether registry host configurations exist for this host
		// in the cache.
		if hostConfigurations, ok := rm.registryHostMap.Load(host); ok {
			return hostConfigurations.([]docker.RegistryHost), nil
		}
		registryHosts := []docker.RegistryHost{}
		// If mirrors exist for this host, create new `RegistryHost` configurations
		// for them.
		if hostConfig, ok := rm.registryConfig.Host[host]; ok {
			for _, mirror := range hostConfig.Mirrors {
				client := rm.client
				scheme := "https"
				if localhost, _ := docker.MatchLocalhost(mirror.Host); localhost || mirror.Insecure {
					scheme = "http"
				}
				if mirror.RequestTimeoutSec > 0 {
					rm.httpConfig.RequestTimeoutMsec = mirror.RequestTimeoutSec * 1000
					if authClient, ok := rm.client.Transport.(*shttp.AuthClient); ok {
						retryClient := shttp.NewRetryableClient(rm.httpConfig)
						client = authClient.CloneWithNewClient(retryClient).StandardClient()
					}
				}
				registryHosts = append(registryHosts, docker.RegistryHost{
					Client:       client,
					Host:         mirror.Host,
					Scheme:       scheme,
					Path:         "/v2",
					Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
				})
			}
		}

		if host == "docker.io" {
			host = "registry-1.docker.io"
		}
		// Create a `RegistryHost` configuration for this host.
		registryHosts = append(registryHosts, docker.RegistryHost{
			Client:       rm.client,
			Host:         host,
			Scheme:       "https",
			Path:         "/v2",
			Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
		})

		// Cache all `RegistryHost` configurations for this host.
		rm.registryHostMap.Store(host, registryHosts)

		return registryHosts, nil
	}
}

// multiCredsFuncs joins a list of credential functions into single credential function.
func multiCredsFuncs(credsFuncs ...Credential) func(string) (string, string, error) {
	return func(host string) (string, string, error) {
		for _, f := range credsFuncs {
			if username, secret, err := f(host); err != nil {
				return "", "", err
			} else if !(username == "" && secret == "") {
				return username, secret, nil
			}
		}
		return "", "", nil
	}
}
