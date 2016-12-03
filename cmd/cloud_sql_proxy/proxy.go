// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// This file contains code for supporting local sockets for the Cloud SQL Proxy.

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"math/rand"
	"time"

	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/fuse"
	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/proxy"
)

// WatchInstances handles the lifecycle of local sockets used for proxying
// local connections.  Values received from the updates channel are
// interpretted as a comma-separated list of instances.  The set of sockets in
// 'dir' is the union of 'instances' and the most recent list from 'updates'.
func WatchInstances(updates <-chan []instanceConfig) (<-chan proxy.Conn, error) {
	ch := make(chan proxy.Conn, 1)
	go watchInstancesLoop(ch, updates)
	return ch, nil
}

type addressToConfigMap map[localServerAddress][]instanceConfig

func createAddressToConfigMap(configs []instanceConfig) (addressToConfigMap) {
		out := make(addressToConfigMap)
		for _, cfg := range configs {
			out[cfg.localServerAddress] = append(out[cfg.localServerAddress], cfg)
		}
		return out
}

func symmetricDiff(lhs addressToConfigMap, rhs addressToConfigMap) ([]localServerAddress, []localServerAddress) {
		var inl []localServerAddress
		var inr []localServerAddress
		for k, _ := range rhs { if _, ok := lhs[k]; !ok { inr = append(inr, k) } }
		for k, _ := range lhs { if _, ok := rhs[k]; !ok { inl = append(inl, k) } }
		return inl, inr
}

type access struct {
	addr localServerAddress
	conn net.Conn
}

func doAcceptLoop(addr localServerAddress, listener net.Listener, out chan<- access) {
	for {
		c, err := listener.Accept()
		if err != nil {
			log.Printf("Error in accept: %v", err)
			listener.Close()
			return
		}
		out <- access { addr: addr, conn: c}
	}
}

func watchInstancesLoop(dst chan<- proxy.Conn, updates <-chan []instanceConfig) {
	listeners := make(map[localServerAddress]net.Listener)
	curConfig := make(addressToConfigMap)
	bridge := make(chan access)
	rand.Seed(time.Now().UnixNano())
	for {
		select {
    case e := <-bridge:
			arr, ok := curConfig[e.addr]
			if ! ok {
				e.conn.Close()
				continue
			}
			cfg := arr[rand.Intn(len(arr))]
			log.Printf("New connection for %q", cfg.Instance)
			dst <- proxy.Conn { cfg.Instance, e.conn }

    case list := <-updates:
			newConfig := createAddressToConfigMap(list)
			closing, opening := symmetricDiff(curConfig, newConfig)

			for _, addr := range closing {
				log.Printf("Closing socket for address %v", addr)
				l := listeners[addr]
				l.Close()
				delete(listeners, addr)
			}
			for _, addr := range opening {
				l, err := listenInstance(addr, newConfig[addr])
				if err != nil {
					log.Printf("Couldn't open socket: %v", err)
					continue
				}
				go doAcceptLoop(addr, l, bridge)
				listeners[addr] = l
			}
			curConfig = newConfig
		}
	}
	
	for _, v := range listeners {
		if err := v.Close(); err != nil {
			log.Printf("Error closing %q: %v", v.Addr(), err)
		}
	}
}

func remove(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("Remove(%q) error: %v", path, err)
	}
}

// listenInstance starts listening on a new unix socket in dir to connect to the
// specified instance. New connections to this socket are sent to dst.
func listenInstance(addr localServerAddress, configs []instanceConfig) (net.Listener, error) {
	unix := addr.Network == "unix"
	if unix {
		remove(addr.Address)
	}
	l, err := net.Listen(addr.Network, addr.Address)
	if err != nil {
		return nil, err
	}
	if unix {
		if err := os.Chmod(addr.Address, 0777|os.ModeSocket); err != nil {
			log.Printf("couldn't update permissions for socket file %q: %v; other users may not be unable to connect", addr.Address, err)
		}
	}
	log.Printf("Listening on %s", addr)
	return l, nil
}

type localServerAddress struct {
	Network, Address string
}

type instanceConfig struct {
	Instance         string
	localServerAddress
}

// loopbackForNet maps a network (e.g. tcp6) to the loopback address for that
// network. It is updated during the initialization of validNets to include a
// valid loopback address for "tcp".
var loopbackForNet = map[string]string{
	"tcp4": "127.0.0.1",
	"tcp6": "[::1]",
}

// validNets tracks the networks that are valid for this platform and machine.
var validNets = func() map[string]bool {
	m := map[string]bool{
		"unix": runtime.GOOS != "windows",
	}

	anyTCP := false
	for _, n := range []string{"tcp4", "tcp6"} {
		addr, ok := loopbackForNet[n]
		if !ok {
			// This is effectively a compile-time error.
			panic(fmt.Sprintf("no loopback address found for %v", n))
		}
		// Open any port to see if the net is valid.
		x, err := net.Listen(n, addr+":0")
		if err != nil {
			// Error is too verbose to be useful.
			continue
		}
		x.Close()
		m[n] = true

		if !anyTCP {
			anyTCP = true
			// Set the loopback value for generic tcp if it hasn't already been
			// set. (If both tcp4/tcp6 are supported the first one in the list
			// (tcp4's 127.0.0.1) is used.
			loopbackForNet["tcp"] = addr
		}
	}
	if anyTCP {
		m["tcp"] = true
	}
	return m
}()

func parseInstanceConfig(dir, instance string) (instanceConfig, error) {
	var ret instanceConfig
	eq := strings.Index(instance, "=")
	if eq != -1 {
		spl := strings.SplitN(instance[eq+1:], ":", 3)
		ret.Instance = instance[:eq]

		switch len(spl) {
		default:
			return ret, fmt.Errorf("invalid %q: expected 'project:instance=tcp:port'", instance)
		case 2:
			// No "host" part of the address. Be safe and assume that they want a
			// loopback address.
			ret.Network = spl[0]
			addr, ok := loopbackForNet[spl[0]]
			if !ok {
				return ret, fmt.Errorf("invalid %q: unrecognized network %v", instance, spl[0])
			}
			ret.Address = fmt.Sprintf("%s:%s", addr, spl[1])
		case 3:
			// User provided a host and port; use that.
			ret.Network = spl[0]
			ret.Address = fmt.Sprintf("%s:%s", spl[1], spl[2])
		}
	} else {
		ret.Instance = instance
		// Default to unix socket.
		ret.Network = "unix"
		ret.Address = filepath.Join(dir, instance)
	}

	if !validNets[ret.Network] {
		return ret, fmt.Errorf("invalid %q: unsupported network: %v", instance, ret.Network)
	}
	return ret, nil
}

// parseInstanceConfigs calls parseInstanceConfig for each instance in the
// provided slice, collecting errors along the way. There may be valid
// instanceConfigs returned even if there's an error.
func parseInstanceConfigs(dir string, instances []string) ([]instanceConfig, error) {
	errs := new(bytes.Buffer)
	var cfg []instanceConfig
	for _, v := range instances {
		if v == "" {
			continue
		}
		if c, err := parseInstanceConfig(dir, v); err != nil {
			fmt.Fprintf(errs, "\n\t%v", err)
		} else {
			cfg = append(cfg, c)
		}
	}

	var err error
	if errs.Len() > 0 {
		err = fmt.Errorf("errors parsing config:%s", errs)
	}
	return cfg, err
}

// CreateInstanceConfigs verifies that the parameters passed to it are valid
// for the proxy for the platform and system and then returns a slice of valid
// instanceConfig.
func CreateInstanceConfigs(dir string, useFuse bool, instances []string, instancesSrc string) ([]instanceConfig, error) {
	if useFuse && !fuse.Supported() {
		return nil, errors.New("FUSE not supported on this system")
	}

	cfgs, err := parseInstanceConfigs(dir, instances)
	if err != nil {
		return nil, err
	}

	if dir == "" {
		// Reasons to set '-dir':
		//    - Using -fuse
		//    - Using the metadata to get a list of instances
		//    - Having an instance that uses a 'unix' network
		if useFuse {
			return nil, errors.New("must set -dir because -fuse was set")
		} else if instancesSrc != "" {
			return nil, errors.New("must set -dir because -instances_metadata was set")
		} else {
			for _, v := range cfgs {
				if v.Network == "unix" {
					return nil, fmt.Errorf("must set -dir: using a unix socket for %v", v.Instance)
				}
			}
		}
		// Otherwise it's safe to not set -dir
	}

	if useFuse {
		if len(instances) != 0 || instancesSrc != "" {
			return nil, errors.New("-fuse is not compatible with -projects, -instances, or -instances_metadata")
		}
		return nil, nil
	}
	// FUSE disabled.
	if len(instances) == 0 && instancesSrc == "" {
		if fuse.Supported() {
			return nil, errors.New("must specify -projects, -fuse, or -instances")
		}
		return nil, errors.New("must specify -projects or -instances")
	}
	return cfgs, nil
}
