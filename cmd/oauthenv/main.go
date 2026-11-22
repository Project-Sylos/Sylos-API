// Command oauthenv encrypts oauth.env values or prints go build -ldflags for the Makefile.
package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"

	"codeberg.org/Sylos/Sylos-API/pkg/oauthobscure"
)

const pkg = "codeberg.org/Sylos/Sylos-API/internal/oauthbuiltin"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "encrypt":
		envPath, keyPath := "oauth.env", "oauth.key"
		if len(os.Args) >= 3 {
			envPath = os.Args[2]
		}
		if len(os.Args) >= 4 {
			keyPath = os.Args[3]
		}
		key, err := oauthobscure.LoadKeyFile(keyPath)
		if err != nil {
			fatal(err)
		}
		if err := oauthobscure.EncryptEnvFileInPlace(envPath, key); err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "encrypted non-empty values in %s\n", envPath)
	case "ldflags":
		envPath, keyPath := "oauth.env", "oauth.key"
		if len(os.Args) >= 3 {
			envPath = os.Args[2]
		}
		if len(os.Args) >= 4 {
			keyPath = os.Args[3]
		}
		key, err := oauthobscure.LoadKeyFile(keyPath)
		if err != nil {
			fatal(err)
		}
		vals, _, err := oauthobscure.LoadEnvFile(envPath, key)
		if err != nil {
			fatal(err)
		}
		fmt.Print(strings.Join(ldflagArgs(vals), " "))
	case "genkey":
		path := "oauth.key"
		if len(os.Args) >= 3 {
			path = os.Args[2]
		}
		if err := writeRandomKey(path); err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", path)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  oauthenv encrypt [oauth.env] [oauth.key]   # rewrite plaintext values as enc:…
  oauthenv ldflags [oauth.env] [oauth.key]   # print -X flags for go build
  oauthenv genkey [oauth.key]                # write a new 32-byte hex key
`)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "oauthenv: %v\n", err)
	os.Exit(1)
}

func ldflagArgs(v oauthobscure.EnvValues) []string {
	var out []string
	add := func(name, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		out = append(out, "-X", pkg+"."+name+"="+value)
	}
	add("DropboxClientID", v.DropboxClientID)
	add("DropboxClientSecret", v.DropboxClientSecret)
	add("BoxClientID", v.BoxClientID)
	add("BoxClientSecret", v.BoxClientSecret)
	add("OneDriveClientID", v.OneDriveClientID)
	add("OneDriveClientSecret", v.OneDriveClientSecret)
	add("OneDriveTenantID", v.OneDriveTenantID)
	add("SharePointClientID", v.SharePointClientID)
	add("SharePointClientSecret", v.SharePointClientSecret)
	add("SharePointTenantID", v.SharePointTenantID)
	return out
}

func writeRandomKey(path string) error {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range key {
		out[i*2] = hexdigits[b>>4]
		out[i*2+1] = hexdigits[b&0x0f]
	}
	out = append(out, '\n')
	return os.WriteFile(path, out, 0o644)
}
