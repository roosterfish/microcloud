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
	"github.com/canonical/microcluster/v2/client"
	"github.com/canonical/microcluster/v2/microcluster"

	"github.com/canonical/microcloud/microcloud/api/types"
	cloudClient "github.com/canonical/microcloud/microcloud/client"
	ovnClient "github.com/canonical/microovn/microovn/client"
)

// OVNService is a MicroOVN service.
type OVNService struct {
	name     string
	address  string
	port     int64
	config   map[string]string
	stateDir string
	proxyF   func(*http.Request) (*url.URL, error)
}

// NewOVNService creates a new MicroOVN service with a client attached.
func NewOVNService(name string, addr string, cloudDir string) (*OVNService, error) {
	return &OVNService{
		name:     name,
		address:  addr,
		port:     OVNPort,
		config:   make(map[string]string),
		stateDir: cloudDir,
		proxyF: func(r *http.Request) (*url.URL, error) {
			if !strings.HasPrefix(r.URL.Path, "/1.0/services/microovn") {
				r.URL.Path = "/1.0/services/microovn" + r.URL.Path
			}

			return shared.ProxyFromEnvironment(r)
		},
	}, nil
}

// Client returns a client to the OVN unix socket.
func (s OVNService) Client() (*client.Client, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return nil, err
	}

	return app.LocalClient()
}

// Bootstrap bootstraps the MicroOVN daemon on the default port.
func (s OVNService) Bootstrap(ctx context.Context) error {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return err
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
			return fmt.Errorf("Timed out waiting for MicroOVN cluster to initialize")
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
func (s OVNService) IssueToken(ctx context.Context, peer string) (string, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return "", err
	}

	return app.NewJoinToken(ctx, peer, 5*time.Minute)
}

// DeleteToken deletes a token by its name.
func (s OVNService) DeleteToken(ctx context.Context, tokenName string, address string) error {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return err
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

		cloudClient.UseAuthProxy(transport, types.MicroOVN, cloudClient.AuthConfig{})
	} else {
		c, err = app.LocalClient()
		if err != nil {
			return err
		}
	}

	return c.DeleteTokenRecord(ctx, tokenName)
}

// Join joins a cluster with the given token.
func (s OVNService) Join(ctx context.Context, joinConfig JoinConfig) error {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return err
	}

	return app.JoinCluster(ctx, s.name, util.CanonicalNetworkAddress(s.address, s.port), joinConfig.Token, joinConfig.OVNConfig)
}

// RemoteClusterMembers returns a map of cluster member names and addresses from the MicroCloud at the given address.
// Provide the certificate of the remote server for mTLS.
func (s OVNService) RemoteClusterMembers(ctx context.Context, cert *x509.Certificate, address string) (map[string]string, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return nil, err
	}

	var client *client.Client

	canonicalAddress := util.CanonicalNetworkAddress(address, CloudPort)
	if cert != nil {
		client, err = app.RemoteClientWithCert(canonicalAddress, cert)
		if err != nil {
			return nil, err
		}
	} else {
		client, err = app.RemoteClient(canonicalAddress)
		if err != nil {
			return nil, err
		}
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("Invalid client transport type")
	}

	cloudClient.UseAuthProxy(transport, types.MicroOVN, cloudClient.AuthConfig{})
	return clusterMembers(ctx, client)
}

// ClusterMembers returns a map of cluster member names and addresses.
func (s OVNService) ClusterMembers(ctx context.Context) (map[string]string, error) {
	client, err := s.Client()
	if err != nil {
		return nil, err
	}

	return clusterMembers(ctx, client)
}

// DeleteClusterMember removes the given cluster member from the service.
func (s OVNService) DeleteClusterMember(ctx context.Context, name string, force bool) error {
	c, err := s.Client()
	if err != nil {
		return err
	}

	return c.DeleteClusterMember(ctx, name, force)
}

// Type returns the type of Service.
func (s OVNService) Type() types.ServiceType {
	return types.MicroOVN
}

// Name returns the name of this Service instance.
func (s OVNService) Name() string {
	return s.name
}

// Address returns the address of this Service instance.
func (s OVNService) Address() string {
	return s.address
}

// Port returns the port of this Service instance.
func (s OVNService) Port() int64 {
	return s.port
}

// GetVersion gets the installed daemon version of the service, and returns an error if the version is not supported.
func (s OVNService) GetVersion(ctx context.Context) (string, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return "", err
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
func (s OVNService) IsInitialized(ctx context.Context) (bool, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return false, err
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
func (s *OVNService) SetConfig(config map[string]string) {
	if s.config == nil {
		s.config = make(map[string]string)
	}

	for key, value := range config {
		s.config[key] = value
	}
}

// SupportsFeature checks if the specified API feature of this Service instance if supported.
func (s *OVNService) SupportsFeature(ctx context.Context, feature string) (bool, error) {
	app, err := ovnClient.NewApp(microcluster.Args{StateDir: s.stateDir, Proxy: s.proxyF})
	if err != nil {
		return false, err
	}

	server, err := app.Status(ctx)
	if err != nil {
		return false, fmt.Errorf("Failed to get MicroOVN server status while checking for features: %v", err)
	}

	if server.Extensions == nil {
		logger.Warnf("MicroOVN server does not expose API extensions")
		return false, nil
	}

	return server.Extensions.HasExtension(feature), nil
}
