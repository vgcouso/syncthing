// Copyright (C) 2014 The Syncthing Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License along
// with this program. If not, see <http://www.gnu.org/licenses/>.

package osutil

// +build windows

import (
	"bytes"
	"net"
	"syscall"
	"unsafe"
)

const (
	MAX_ADAPTER_NAME_LENGTH        = 256
	MAX_ADAPTER_DESCRIPTION_LENGTH = 128
	MAX_ADAPTER_ADDRESS_LENGTH     = 8
)

type ipAddressString struct {
	next    *ipAddressString
	address [16]byte
	mask    [16]byte
	context uint32
}

type ipAdapterInfo struct {
	next               *ipAdapterInfo
	comboIndex         uint32
	adapterName        [MAX_ADAPTER_NAME_LENGTH + 4]byte
	adapterDescription [MAX_ADAPTER_DESCRIPTION_LENGTH + 4]byte
	addressLength      uint32
	address            [MAX_ADAPTER_ADDRESS_LENGTH]byte
	adapterIndex       uint32
	adapterType        uint32
	dhcp               uint32
	currentAddress     *ipAddressString
	ipAddress          ipAddressString
	gatewayAddress     ipAddressString
	dhcpServer         ipAddressString
	haveWins           bool
	primaryWins        ipAddressString
	secondaryWins      ipAddressString
	leaseObtained      uint64
	leaseExpires       uint64
}

var (
	getAdaptersInfo = syscall.NewLazyDLL("Iphlpapi.dll").NewProc("GetAdaptersInfo")
)

func GetGateways() ([]net.IP, error) {
	err := getAdaptersInfo.Find()
	if err != nil {
		return nil, err
	}

	adapters := [16]ipAdapterInfo{}
	size := unsafe.Sizeof(adapters)

	r0, _, err := getAdaptersInfo.Call(uintptr(unsafe.Pointer(&adapters[0])), uintptr(unsafe.Pointer(&size)))
	if r0 != 0 {
		return nil, err
	}

	gateways := []net.IP{}
	adapter := &adapters[0]
	for adapter != nil {
		gateway := &adapter.gatewayAddress
		for gateway != nil {
			ip := net.ParseIP(sliceToString(gateway.address[:]))
			if !ip.IsUnspecified() {
				gateways = append(gateways, ip)
			}
			gateway = gateway.next
		}
		adapter = adapter.next
	}

	return gateways, nil
}

func sliceToString(slice []byte) string {
	n := bytes.IndexByte(slice, 0)
	return string(slice[:n])
}
