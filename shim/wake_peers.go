package shim

import (
	"bufio"
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/containerd/log"
	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const wakePeerDialTimeout = 5 * time.Second

// wakePeers dials peer services so their zeropod activators restore in
// parallel with this container. Names are resolved through the pod's DNS
// (e.g. CoreDNS) since the shim runs with the host's resolv.conf, which
// cannot resolve cluster service names.
func (c *Container) wakePeers(ctx context.Context) {
	if len(c.cfg.WakePeers) == 0 {
		return
	}
	dialer := &net.Dialer{Timeout: wakePeerDialTimeout, Resolver: podResolver(ctx, c.cfg.Spec)}
	for _, peer := range c.cfg.WakePeers {
		go func(addr string) {
			start := time.Now()
			err := c.netNS.Do(func(_ ns.NetNS) error {
				conn, err := dialer.Dial("tcp", addr)
				if err != nil {
					return err
				}
				// Send a byte so the peer's activator doesn't classify this
				// as a kube-probe (bare connect+close) and skip the restore.
				_, _ = conn.Write([]byte{0})
				return conn.Close()
			})
			if err != nil {
				log.G(ctx).Warnf("wake-peer %s: %s", addr, err)
				return
			}
			log.G(ctx).Infof("wake-peer %s dialed in %s", addr, time.Since(start))
		}(peer)
	}
}

func podResolver(ctx context.Context, spec *specs.Spec) *net.Resolver {
	nameserver := podNameserver(spec)
	if nameserver == "" {
		log.G(ctx).Warn("wake-peers: no pod nameserver found, using host resolver")
		return nil
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, net.JoinHostPort(nameserver, "53"))
		},
	}
}

func podNameserver(spec *specs.Spec) string {
	if spec == nil {
		return ""
	}
	for _, m := range spec.Mounts {
		if m.Destination != "/etc/resolv.conf" {
			continue
		}
		f, err := os.Open(m.Source)
		if err != nil {
			return ""
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) >= 2 && fields[0] == "nameserver" {
				return fields[1]
			}
		}
	}
	return ""
}
