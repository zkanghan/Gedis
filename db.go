package main

import "strconv"

func LookupKey(key *GObj) *GObj {
	expireIfNeeded(key)
	entry := server.db.data.Find(key)

	if entry == nil {
		return nil
	}
	return entry.Val
}

func GetNumber(s string, target *int64) (err error) {
	*target, err = strconv.ParseInt(s, 10, 64)
	return err
}
