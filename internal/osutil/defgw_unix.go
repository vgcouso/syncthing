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

// Unix specific code, but builds on all platforms to get the tests executed.

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
)

var exps = []*regexp.Regexp{
	// Linux
	regexp.MustCompile(`^0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)\s`),
	// Mac, FreeBSD
	regexp.MustCompile(`^default\s+(\d+\.\d+\.\d+\.\d+)\s`),
}

func parseNetstatDefGW(output []byte) (net.IP, error) {
	for _, line := range bytes.Split(output, []byte("\n")) {
		for _, exp := range exps {
			if m := exp.FindSubmatch(line); m != nil {
				addr, err := net.ResolveIPAddr("ip", string(m[1]))
				if err != nil {
					return nil, err
				}
				return addr.IP, nil
			}
		}
	}
	return nil, errors.New("unrecognized netstat format")
}

func parseProcNetRoute(r io.Reader) ([]net.IP, error) {
	var gws []net.IP
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := s.Bytes()
		fields := bytes.Fields(line)
		if len(fields) < 3 {
			continue
		}
		if bytes.Compare(fields[1], []byte("00000000")) == 0 {
			ipAsInt, err := strconv.ParseInt(string(fields[2]), 16, 32)
			if err != nil {
				return nil, err
			}
			gws = append(gws, net.IP{byte(ipAsInt), byte(ipAsInt >> 8), byte(ipAsInt >> 16), byte(ipAsInt >> 24)})
		}
	}
	return gws, nil
}
