package task

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDigestIgnoresObjectOrderButTracksContent(t *testing.T) {
	decode := func(input string) Submission {
		t.Helper()
		var value Submission
		decoder := json.NewDecoder(strings.NewReader(input))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	a := decode(`{"source_system":"shop","vendor_code":"ads","event_type":"signup","payload":{"user_id":9007199254740993,"click_id":"c1"}}`)
	b := decode(`{"payload":{"click_id":"c1","user_id":9007199254740993},"event_type":"signup","vendor_code":"ads","source_system":"shop"}`)
	x, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.Digest()
	if err != nil || x != y {
		t.Fatalf("same request has different hash: %s %s %v", x, y, err)
	}
	b.Payload["user_id"] = json.Number("9007199254740992")
	z, err := b.Digest()
	if err != nil || x == z {
		t.Fatal("different content must have a different hash")
	}
}
