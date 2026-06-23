package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Config combines transport security settings.
type Config struct {
	Protocol SecurityProtocol
	TLS      TLSConfig
	SASL     SASLConfig
}

type SecurityProtocol string

const (
	SecurityPlaintext     SecurityProtocol = "PLAINTEXT"
	SecuritySSL           SecurityProtocol = "SSL"
	SecuritySASLPlaintext SecurityProtocol = "SASL_PLAINTEXT"
	SecuritySASLSSL       SecurityProtocol = "SASL_SSL"
)

type SASLMechanism string

const (
	SASLPlain    SASLMechanism = "PLAIN"
	SASLSCRAM256 SASLMechanism = "SCRAM-SHA-256"
	SASLSCRAM512 SASLMechanism = "SCRAM-SHA-512"
	SASLGSSAPI   SASLMechanism = "GSSAPI"
	SASLOAuth    SASLMechanism = "OAUTHBEARER"
)

type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
	ServerName         string
}

type SASLConfig struct {
	Mechanism SASLMechanism
	Username  string
	Password  string
	Token     string
	// TokenProvider supplies OAuth bearer tokens before connect and on reconnect.
	TokenProvider OAuthTokenProvider
	// Kerberos / GSSAPI (requires external krb5 tooling or future optional module).
	Kerberos KerberosConfig
}

// OAuthTokenProvider returns a fresh OAuth bearer token (OIDC / cloud IdP).
type OAuthTokenProvider func(ctx context.Context) (token string, err error)

// GSSAPITokenProvider exchanges SPNEGO tokens with an external Kerberos stack (kinit, krb5, AD).
type GSSAPITokenProvider func(ctx context.Context, challenge []byte) ([]byte, error)

// KerberosConfig holds GSSAPI/Kerberos SASL settings.
type KerberosConfig struct {
	Principal string // e.g. kafka/client@REALM
	Keytab    string // path to keytab file (reserved; use TokenProvider)
	Realm     string
	Service   string // krb5 service name, default "kafka"
	// InitToken is an optional first SPNEGO token (e.g. from kinit).
	InitToken []byte
	// TokenProvider handles multi-round SPNEGO when the broker returns a challenge.
	TokenProvider GSSAPITokenProvider
}

func (c Config) SASLEnabled() bool {
	return c.Protocol == SecuritySASLPlaintext || c.Protocol == SecuritySASLSSL
}

func (c Config) TLSEnabled() bool {
	return c.Protocol == SecuritySSL || c.Protocol == SecuritySASLSSL || c.TLS.Enabled
}

func Dial(ctx context.Context, d net.Dialer, addr string, sec Config) (net.Conn, error) {
	if !sec.TLSEnabled() {
		return d.DialContext(ctx, "tcp", addr)
	}
	tlsCfg, err := BuildTLSConfig(sec.TLS)
	if err != nil {
		return nil, err
	}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(raw, tlsCfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tlsConn, nil
}

func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		ServerName:         cfg.ServerName,
	}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("auth: invalid CA")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

type requester interface {
	Request(ctx context.Context, apiKey, apiVersion int16, body []byte) ([]byte, error)
}

// Handshake performs SASL authentication on an established connection.
func Handshake(ctx context.Context, conn requester, sec Config) error {
	mech := string(sec.SASL.Mechanism)
	if mech == "" {
		mech = "PLAIN"
	}
	body := encodeSaslHandshake(mech)
	resp, err := conn.Request(ctx, protocol.APISaslHandshake, protocol.VerSaslHandshake, body)
	if err != nil {
		return err
	}
	rb, err := protocol.ResponseBody(resp)
	if err != nil {
		return err
	}
	if err := decodeSaslHandshakeErr(rb); err != nil {
		return err
	}

	switch sec.SASL.Mechanism {
	case SASLSCRAM256:
		return scramSHA256(ctx, conn, sec)
	case SASLSCRAM512:
		return scramSHA512(ctx, conn, sec)
	case SASLGSSAPI:
		return gssapi(ctx, conn, sec)
	case SASLOAuth:
		token := sec.SASL.Token
		if token == "" && sec.SASL.TokenProvider != nil {
			var err error
			token, err = sec.SASL.TokenProvider(ctx)
			if err != nil {
				return fmt.Errorf("auth: oauth token provider: %w", err)
			}
		}
		if token == "" {
			return fmt.Errorf("auth: OAUTHBEARER requires Token or TokenProvider")
		}
		return saslAuthenticate(ctx, conn, buildOAuthMessage(token))
	default:
		return saslAuthenticate(ctx, conn, buildPlainMessage(sec.SASL.Username, sec.SASL.Password))
	}
}

func encodeSaslHandshake(mechanism string) []byte {
	b := make([]byte, 2+len(mechanism))
	binary.BigEndian.PutUint16(b, uint16(len(mechanism)))
	copy(b[2:], mechanism)
	return b
}

func decodeSaslHandshakeErr(body []byte) error {
	if len(body) < 2 {
		return errors.New("auth: short sasl handshake response")
	}
	code := int16(binary.BigEndian.Uint16(body))
	if code != 0 {
		return fmt.Errorf("auth: sasl handshake error code %d", code)
	}
	return nil
}

func buildPlainMessage(user, pass string) []byte {
	msg := []byte("\x00" + user + "\x00" + pass)
	out := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(out, uint32(len(msg)))
	copy(out[4:], msg)
	return out
}

func buildOAuthMessage(token string) []byte {
	msg := []byte("n,,")
	msg = append(msg, []byte(`auth=Bearer `+token)...)
	out := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(out, uint32(len(msg)))
	copy(out[4:], msg)
	return out
}

func saslAuthenticate(ctx context.Context, conn requester, payload []byte) error {
	resp, err := conn.Request(ctx, protocol.APISaslAuthenticate, protocol.VerSaslAuthenticate, payload)
	if err != nil {
		return err
	}
	rb, err := protocol.ResponseBody(resp)
	if err != nil {
		return err
	}
	if len(rb) < 6 {
		return errors.New("auth: short sasl authenticate response")
	}
	code := int16(binary.BigEndian.Uint16(rb[4:6]))
	if code != 0 {
		return fmt.Errorf("auth: sasl authenticate failed code %d", code)
	}
	return nil
}
