package main

import (
	"fmt"
	"strings"
)

type command struct {
	verb   string
	key    string
	value  []byte
	flags  uint32
	expire int32
	casid  uint64
	delta  int
}

//Support Get Set Add Replace Delete DeleteAll Incr Decr Cas 
func decode(req []byte) command, err {
	ret := command{}
	var length int
	op := string(req)
	if strings.HasPrefix(op, "flush_all") {
		ret.verb = "flush_all"
	} else if strings.HasPrefix(op, "delete") {
		fmt.Sscanf(op, "%s %s\r\n", &ret.verb, &ret.key)
	} else if strings.HasPrefix(op, "incr") || strings.HasPrefix(op, "decr") {
		fmt.Sscanf(op, "%s %s %d\r\n", &ret.verb, &ret.key, &ret.delta)
	} else if strings.HasPrefix(op, "cas") {
		fmt.Sscanf(op, "%s %s %d %d %d %d\r\n%s",
			&ret.verb, &ret.key, &ret.flags, &ret.expire, &length, &ret.casid, &s)
		ret.value = []byte(s)
	} else {
		fmt.Sscanf(op, "%s %s %d %d %d %d\r\n%s",
			&ret.verb, &ret.key, &ret.flags, &ret.expire, &length, &s)
		ret.value = []byte(s)
	}
	if len(ret.value) != length {
		return ret, errors.New("Length in header and the length of value are not equal")
	} else {
		return ret, nil
	}
}
