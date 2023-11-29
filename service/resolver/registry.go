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

type Credential func(string) (string, string, error)
type RegistryManager struct {
	// client is the global HTTP client to be shared across hosts
	client          *http.Client
	httpConfig      config.RetryableHTTPClientConfig
	registryConfig  config.ResolverConfig
	registryHostMap *sync.Map
}

type RegistryManagerOpt func(*RegistryManager)

func GlobalHeader() http.Header {
	header := http.Header{}
	header.Set("User-Agent", fmt.Sprintf("soci-snapshotter/%s", version.Version))
	return header
}

// NewRegistryManager
func NewRegistryManager(httpConfig config.RetryableHTTPClientConfig, registryConfig config.ResolverConfig, credsFuncs []Credential) *RegistryManager {
	regMngr := &RegistryManager{
		httpConfig:      httpConfig,
		registryConfig:  registryConfig,
		registryHostMap: &sync.Map{},
	}
	authClientOpts := []shttp.AuthClientOpt{shttp.WithCredentialProvider(multiCredsFuncs(credsFuncs...)), shttp.WithHeader(GlobalHeader())}
	regMngr.client = shttp.NewStandardAuthClient(httpConfig, authClientOpts...)
	return regMngr
}

// ConfigureRegistries
func (rm *RegistryManager) ConfigureRegistries() docker.RegistryHosts {
	return func(host string) ([]docker.RegistryHost, error) {
		if host == "docker.io" {
			host = "registry-1.docker.io"
		}
		registryHosts := []docker.RegistryHost{}

		// Check whether registry host configurations exist for this host
		// in the cache.
		if hostConfigurations, ok := rm.registryHostMap.Load(host); ok {
			return hostConfigurations.([]docker.RegistryHost), nil
		}
		// If mirrors exist for this host, create new `RegistryHost` configurations
		// for them.
		if hostConfig, ok := rm.registryConfig.Host[host]; ok {
			for _, mirror := range hostConfig.Mirrors {
				var client *http.Client
				scheme := "https"
				if localhost, _ := docker.MatchLocalhost(mirror.Host); localhost || mirror.Insecure {
					scheme = "http"
				}
				if mirror.RequestTimeoutSec > 0 {
					rm.httpConfig.RequestTimeoutMsec = mirror.RequestTimeoutSec * 1000
					if globalAuthClient, ok := rm.client.Transport.(*shttp.AuthClient); ok {
						client = globalAuthClient.Clone(rm.httpConfig)
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
		// Create a `RegistryHost` configuration for this specific host.
		registryHosts = append(registryHosts, docker.RegistryHost{
			Client:       rm.client,
			Host:         host,
			Scheme:       "https",
			Path:         "/v2",
			Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
		})

		// Cache all `RegistryHost` configurations for this host
		rm.registryHostMap.Store(host, registryHosts)

		return registryHosts, nil
	}
}

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

// // RegistryHostsFromConfig creates RegistryHosts (a set of registry configuration) from Config.
// func RegistryHostsFromConfig(registryConfig config.ResolverConfig, httpConfig config.RetryableHTTPClientConfig, credsFuncs ...Credential) source.RegistryHosts {
// 	return func(ref reference.Spec) (hosts []docker.RegistryHost, _ error) {
// 		host := ref.Hostname()
// 		for _, h := range append(registryConfig.Host[host].Mirrors, config.MirrorConfig{
// 			Host: host,
// 		}) {
// 			if h.RequestTimeoutSec < 0 {
// 				httpConfig.RequestTimeoutMsec = 0
// 			}
// 			if h.RequestTimeoutSec > 0 {
// 				httpConfig.RequestTimeoutMsec = h.RequestTimeoutSec * 1000
// 			}
// 			client := socihttp.NewRetryableClient(httpConfig)
// 			config := docker.RegistryHost{
// 				Client:       client,
// 				Host:         h.Host,
// 				Scheme:       "https",
// 				Path:         "/v2",
// 				Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
// 				Authorizer: docker.NewDockerAuthorizer(
// 					docker.WithAuthClient(client),
// 					docker.WithAuthCreds(multiCredsFuncs(ref, credsFuncs...))),
// 			}
// 			if localhost, _ := docker.MatchLocalhost(config.Host); localhost || h.Insecure {
// 				config.Scheme = "http"
// 			}
// 			if config.Host == "docker.io" {
// 				config.Host = "registry-1.docker.io"
// 			}
// 			hosts = append(hosts, config)
// 		}
// 		return
// 	}
// }
