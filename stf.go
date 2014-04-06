package stf

import (
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const VERSION = "0.0.1"

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func GenerateRandomId(hint string, length int) string {
	h := sha1.New()
	io.WriteString(h, hint)
	io.WriteString(h, strconv.FormatInt(time.Now().UTC().UnixNano(), 10))
	return (fmt.Sprintf("%x", h.Sum(nil)))[0:length]
}

func GetHome() string {
	home := os.Getenv("STF_HOME")
	if home == "" {
		var err error
		home, err = os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get home from env and Getwd: %s", err)
		}
	}
	return home
}
