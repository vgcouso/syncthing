package osutil

// +build darwin linux freebsd openbsd solaris

import (
	"net"
	"testing"
)

var gwTestcases = []struct {
	data []byte
	gw   net.IP
}{
	// Linux
	{[]byte(`Kernel IP routing table
Destination     Gateway         Genmask         Flags   MSS Window  irtt Iface
0.0.0.0         172.16.32.1     0.0.0.0         UG        0 0          0 eth0
172.16.32.0     0.0.0.0         255.255.255.0   U         0 0          0 eth0
`), net.IP{172, 16, 32, 1}},

	// Mac
	{[]byte(`Routing tables

Internet:
Destination        Gateway            Flags        Refs      Use   Netif Expire
default            172.16.32.3        UGSc           47        0     en0
127                127.0.0.1          UCS             0        4     lo0
127.0.0.1          127.0.0.1          UH              9  1050839     lo0
169.254            link#4             UCS             0        0     en0
172.16.32/24       link#4             UCS            11        0     en0
172.16.32.1/32     link#4             UCS             1        0     en0
172.16.32.1        0:25:90:38:94:4    UHLWIir        49       26     en0   1160
172.16.32.3        0:14:4f:e7:39:6    UHLWI           0        0     en0   1149
172.16.32.6        b2:d2:27:af:cb:fd  UHLWI           0        0     en0   1121
172.16.32.7        72:2d:86:4c:26:39  UHLWI           0        0     en0   1120
`), net.IP{172, 16, 32, 3}},

	// Solaris
	{[]byte(`
Routing Table: IPv4
  Destination           Gateway           Flags  Ref     Use     Interface
-------------------- -------------------- ----- ----- ---------- ---------
default              172.16.32.4          UG      214   31907608 net0
127.0.0.1            127.0.0.1            UH        2       1840 lo0
172.16.32.0          172.16.32.17         U         5     108360 net0
`), net.IP{172, 16, 32, 4}},
}

func TestParseNetstatDefGW(t *testing.T) {
	for _, tc := range gwTestcases {
		gw, err := parseNetstatDefGW(tc.data)
		if err != nil {
			t.Error(err)
			continue
		}
		if !gw.Equal(tc.gw) {
			t.Errorf("Incorrect gw %v != %v", gw, tc.gw)
		}
	}
}
