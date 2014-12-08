package osutil

// +build darwin linux freebsd openbsd solaris

import (
	"bytes"
	"errors"
	"net"
	"regexp"
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
