// Command security demonstrates how to secure the connection to Kafka with TLS
// and SASL authentication using gokafka.WithSecurity.
//
// Kafka authenticates and encrypts traffic through a "security protocol" that
// combines two independent choices:
//
//   - Transport: plaintext TCP, or TLS (encrypted + broker identity verified).
//   - Authentication: none, or SASL (a challenge/response login handshake).
//
// gokafka exposes the four standard combinations as SecurityProtocol constants:
//
//   - gokafka.SecurityPlaintext     — no TLS, no auth (dev only; never in prod).
//   - gokafka.SecuritySSL           — TLS transport, no SASL login (mTLS-style,
//     where the client cert itself is the identity).
//   - gokafka.SecuritySASLPlaintext — SASL login, but over an UNENCRYPTED wire.
//     Credentials travel in the clear for PLAIN — avoid outside a trusted network.
//   - gokafka.SecuritySASLSSL       — SASL login over TLS. This is the recommended
//     production setup: the wire is encrypted AND the client proves who it is.
//
// SASL itself has several mechanisms, also exposed as constants:
//
//   - gokafka.SASLPlain    — username/password sent (base64) to the broker. Only
//     safe over TLS (SASL_SSL), because PLAIN offers no protection on its own.
//   - gokafka.SASLSCRAM256 — SCRAM-SHA-256: salted challenge/response; the password
//     is never sent over the wire. A good default for username/password auth.
//   - gokafka.SASLSCRAM512 — SCRAM-SHA-512: same design, stronger hash.
//   - gokafka.SASLOAuth     — OAUTHBEARER: a bearer token from an OIDC/cloud IdP
//     (set SASL.Token or SASL.TokenProvider instead of a password).
//   - gokafka.SASLGSSAPI    — Kerberos/GSSAPI for enterprise/AD environments.
//
// This example is illustrative and config-only: it builds a fully-configured
// secured client and prints the intended security settings. It will then attempt
// to connect, which only succeeds against a broker actually configured for
// SASL_SSL with matching credentials — so we treat a connect failure as expected
// and just report it rather than crashing.
package main

import (
	"log"
	"os"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}

	// Credentials come from the environment so they never live in source control.
	user := env("KAFKA_USER", "demo-user")
	password := env("KAFKA_PASSWORD", "demo-password")

	// Build the security configuration.
	//
	// SecuritySASLSSL selects "SASL login over TLS": gokafka opens a TLS
	// connection first (encrypting everything and verifying the broker's
	// certificate), then performs the SASL handshake to authenticate.
	sec := gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLSSL,

		// TLS settings. With no CAFile set, gokafka verifies the broker
		// certificate against the host's system trust store. Point CAFile at a
		// PEM bundle to trust a private/self-signed CA instead. CertFile+KeyFile
		// enable mutual TLS (the client presents its own certificate).
		TLS: gokafka.TLSConfig{
			Enabled:  true,
			CAFile:   os.Getenv("KAFKA_CA_FILE"), // optional: custom CA bundle
			CertFile: os.Getenv("KAFKA_CERT_FILE"),
			KeyFile:  os.Getenv("KAFKA_KEY_FILE"),
			// ServerName overrides the hostname used for certificate validation
			// (useful when connecting via an IP or through a proxy).
			ServerName: os.Getenv("KAFKA_TLS_SERVERNAME"),
			// InsecureSkipVerify: true, // DEV ONLY — disables cert verification.
		},

		// SASL login. SCRAM-SHA-256 is a solid password-based default: the
		// password is used to answer a salted challenge and is never transmitted.
		// Switch Mechanism to gokafka.SASLPlain, gokafka.SASLSCRAM512, etc. as
		// your broker requires.
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLSCRAM256,
			Username:  user,
			Password:  password,
		},
	}

	// Assemble the client config with the security options applied.
	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-security-example"),
		gokafka.WithSecurity(sec),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Print the intended configuration so the reader can see exactly what was
	// requested — without ever logging the password.
	log.Printf("security configuration:")
	log.Printf("  brokers        = %v", brokers)
	log.Printf("  protocol       = %s (TLS transport + SASL authentication)", sec.Protocol)
	log.Printf("  TLS enabled    = %t", sec.TLS.Enabled)
	log.Printf("  TLS CA file    = %q (empty = system trust store)", sec.TLS.CAFile)
	log.Printf("  TLS client cert= %q (set with key for mutual TLS)", sec.TLS.CertFile)
	log.Printf("  SASL mechanism = %s", sec.SASL.Mechanism)
	log.Printf("  SASL username  = %s", sec.SASL.Username)
	log.Printf("  SASL password  = %s", mask(sec.SASL.Password))

	// Creating the client dials the brokers and runs the TLS + SASL handshake.
	// Against a broker that is not configured for SASL_SSL (or with wrong
	// credentials) this fails — which is expected for this illustrative example.
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Printf("could not establish a secured connection (expected unless a "+
			"SASL_SSL broker with matching credentials is running): %v", err)
		return
	}
	defer client.Close()

	log.Printf("secured connection established: TLS + SASL(%s) handshake succeeded", sec.SASL.Mechanism)
}

// mask hides all but the presence of a secret so it is never printed in logs.
func mask(s string) string {
	if s == "" {
		return "(unset)"
	}
	return "***"
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
