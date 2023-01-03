package test

import (
	"fmt"
	"strings"
	"testing"
)

func TestClient(t *testing.T) {
	s := ""
	index := strings.Index(s, "\r\n")
	fmt.Println(index)
}
