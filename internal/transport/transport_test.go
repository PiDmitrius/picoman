package transport

import "testing"

func TestAPIErrorIncludesCode(t *testing.T) {
	err := (&APIError{Platform: "telegram", Code: 400, Description: "bad request"}).Error()
	if err != "telegram 400: bad request" {
		t.Fatalf("error = %q", err)
	}
}
