package db

type bmEntry struct {
	name  string
	index int32
}

type bmList struct {
	entries []bmEntry
}
