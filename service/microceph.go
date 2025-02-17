package service

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	cephTypes "github.com/canonical/microceph/microceph/api/types"
	cephClient "github.com/canonical/microceph/microceph/client"
	"github.com/canonical/microcluster/v2/client"
	"github.com/canonical/microcluster/v2/microcluster"

	"github.com/canonical/microcloud/microcloud/api/types"
	cloudClient "github.com/canonical/microcloud/microcloud/client"
)

// CephService is a MicroCeph service.
type CephService struct {
	name     string
	address  string
	port     int64
	config   map[string]string
	stateDir string
	proxyF   func(*http.Request) (*url.URL, error)
}

// NewCephService creates a new MicroCeph service with a client attached.
func NewCephService(name string, addr string, cloudDir string) (*CephService, error) {
	return &CephService{
		name:     name,
		address:  addr,
		port:     CephPort,
		config:   make(map[string]string),
		stateDir: cloudDir,
		proxyF: func(r *http.Request) (*url.URL, error) {
			if !strings.HasPrefix(r.URL.Path, "/1.0/services/microceph") {
				r.URL.Path = "/1.0/services/microceph" + r.URL.Path
			}

			return shared.ProxyFromEnvironment(r)
		},
	}, nil
}

// Client returns a client to the Ceph unix socket. If target is specified, it will be added to the query params.
func (s CephService) Client(target string) (*client.Client, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return nil, err
	}

	c, err := app.LocalClient()
	if err != nil {
		return nil, fmt.Errorf("Failed creating MicroCeph local client: %w")
	}

	if target != "" {
		c.UseTarget(target)
	}

	transport, ok := c.Transport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("Invalid client transport type")
	}

	cloudClient.UseAuthProxy(transport, types.MicroCeph, cloudClient.AuthConfig{})
	return c, nil
}

// Bootstrap bootstraps the MicroCeph daemon on the default port.
func (s CephService) Bootstrap(ctx context.Context) error {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	err = app.NewCluster(ctx, s.name, util.CanonicalNetworkAddress(s.address, s.port), s.config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Timed out waiting for MicroCeph cluster to initialize")
		default:
			names, err := s.ClusterMembers(ctx)
			if err != nil {
				return err
			}

			if len(names) > 0 {
				return nil
			}

			time.Sleep(time.Second)
		}
	}
}

// IssueToken issues a token for the given peer. Each token will last 5 minutes in case the system joins the cluster very slowly.
func (s CephService) IssueToken(ctx context.Context, peer string) (string, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return "", fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	return app.NewJoinToken(ctx, peer, 5*time.Minute)
}

// DeleteToken deletes a token by its name.
func (s CephService) DeleteToken(ctx context.Context, tokenName string, address string) error {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	var c *client.Client
	if address != "" {
		c, err = app.RemoteClient(util.CanonicalNetworkAddress(address, CloudPort))
		if err != nil {
			return err
		}

		transport, ok := c.Transport.(*http.Transport)
		if !ok {
			return fmt.Errorf("Invalid client transport type")
		}

		cloudClient.UseAuthProxy(transport, types.MicroCeph, cloudClient.AuthConfig{})
	} else {
		c, err = app.LocalClient()
	}

	if err != nil {
		return err
	}

	return c.DeleteTokenRecord(ctx, tokenName)
}

// Join joins a cluster with the given token.
func (s CephService) Join(ctx context.Context, joinConfig JoinConfig) error {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	err = app.JoinCluster(ctx, s.name, util.CanonicalNetworkAddress(s.address, s.port), joinConfig.Token, nil)
	if err != nil {
		return err
	}

	c, err := s.Client("")
	if err != nil {
		return err
	}

	for _, disk := range joinConfig.CephConfig {
		_, err := cephClient.AddDisk(ctx, c, &disk)
		if err != nil {
			return err
		}
	}

	return nil
}

// remoteClient returns an https client for the given address:port.
// It picks the cluster certificate if none is provided to verify the remote.
func (s CephService) remoteClient(cert *x509.Certificate, address string) (*client.Client, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return nil, fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	var client *client.Client

	canonicalAddress := util.CanonicalNetworkAddress(address, CloudPort)
	if cert != nil {
		client, err = app.RemoteClientWithCert(canonicalAddress, cert)
	} else {
		client, err = app.RemoteClient(canonicalAddress)
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}

// RemoteClusterMembers returns a map of cluster member names and addresses from the MicroCloud at the given address.
// Provide the certificate of the remote server for mTLS.
func (s CephService) RemoteClusterMembers(ctx context.Context, cert *x509.Certificate, address string) (map[string]string, error) {
	client, err := s.remoteClient(cert, address)
	if err != nil {
		return nil, err
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("Invalid client transport type")
	}

	cloudClient.UseAuthProxy(transport, types.MicroCeph, cloudClient.AuthConfig{})
	return clusterMembers(ctx, client)
}

// ClusterMembers returns a map of cluster member names and addresses.
func (s CephService) ClusterMembers(ctx context.Context) (map[string]string, error) {
	client, err := s.Client("")
	if err != nil {
		return nil, err
	}

	return clusterMembers(ctx, client)
}

// DeleteClusterMember removes the given cluster member from the service.
func (s CephService) DeleteClusterMember(ctx context.Context, name string, force bool) error {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	c, err := app.LocalClient()
	if err != nil {
		return err
	}

	return c.DeleteClusterMember(ctx, name, force)
}

// ClusterConfig returns the Ceph cluster configuration.
func (s CephService) ClusterConfig(ctx context.Context, targetAddress string, cert *x509.Certificate) (map[string]string, error) {
	var c *client.Client
	var err error
	if targetAddress == "" {
		c, err = s.Client("")
		if err != nil {
			return nil, err
		}
	} else {
		c, err = s.remoteClient(cert, targetAddress)
		if err != nil {
			return nil, err
		}

		transport, ok := c.Transport.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("Invalid client transport type")
		}

		cloudClient.UseAuthProxy(transport, types.MicroCeph, cloudClient.AuthConfig{})
	}

	data := cephTypes.Config{}
	cs, err := cephClient.GetConfig(ctx, c, &data)
	if err != nil {
		return nil, err
	}

	configs := make(map[string]string, len(cs))
	for _, c := range cs {
		configs[c.Key] = c.Value
	}

	return configs, nil
}

// Type returns the type of Service.
func (s CephService) Type() types.ServiceType {
	return types.MicroCeph
}

// Name returns the name of this Service instance.
func (s CephService) Name() string {
	return s.name
}

// Address returns the address of this Service instance.
func (s CephService) Address() string {
	return s.address
}

// Port returns the port of this Service instance.
func (s CephService) Port() int64 {
	return s.port
}

// GetVersion gets the installed daemon version of the service, and returns an error if the version is not supported.
func (s CephService) GetVersion(ctx context.Context) (string, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return "", fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	status, err := app.Status(ctx)
	if err != nil && api.StatusErrorCheck(err, http.StatusNotFound) {
		return "", fmt.Errorf("The installed version of %s is not supported", s.Type())
	}

	if err != nil {
		return "", err
	}

	err = validateVersion(s.Type(), status.Version)
	if err != nil {
		return "", err
	}

	return status.Version, nil
}

// IsInitialized returns whether the service is initialized.
func (s CephService) IsInitialized(ctx context.Context) (bool, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return false, fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	err = app.Ready(ctx)
	if err != nil && api.StatusErrorCheck(err, http.StatusNotFound) {
		return false, fmt.Errorf("Unix socket not found. Check if %s is installed", s.Type())
	}

	if err != nil {
		return false, fmt.Errorf("Failed to wait for %s to get ready: %w", s.Type(), err)
	}

	status, err := app.Status(ctx)
	if err != nil {
		return false, fmt.Errorf("Failed to get %s status: %w", s.Type(), err)
	}

	return status.Ready, nil
}

// SetConfig sets the config of this Service instance.
func (s *CephService) SetConfig(config map[string]string) {
	if s.config == nil {
		s.config = make(map[string]string)
	}

	for key, value := range config {
		s.config[key] = value
	}
}

// SupportsFeature checks if the specified API feature of this Service instance if supported.
func (s *CephService) SupportsFeature(ctx context.Context, feature string) (bool, error) {
	app, err := cephClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return false, fmt.Errorf("Failed creating MicroCeph instance: %w")
	}

	server, err := app.Status(ctx)
	if err != nil {
		return false, fmt.Errorf("Failed to get MicroCeph server status while checking for features: %v", err)
	}

	if server.Extensions == nil {
		logger.Warnf("MicroCeph server does not expose API extensions")
		return false, nil
	}

	return server.Extensions.HasExtension(feature), nil
}
