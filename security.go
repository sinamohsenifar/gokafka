package gokafka

import "github.com/sinamohsenifar/gokafka/internal/auth"

type (
	SecurityConfig        = auth.Config
	TLSConfig             = auth.TLSConfig
	SASLConfig            = auth.SASLConfig
	KerberosConfig        = auth.KerberosConfig
	GSSAPITokenProvider   = auth.GSSAPITokenProvider
	SecurityProtocol      = auth.SecurityProtocol
	SASLMechanism         = auth.SASLMechanism
)

const (
	SecurityPlaintext     = auth.SecurityPlaintext
	SecuritySSL           = auth.SecuritySSL
	SecuritySASLPlaintext = auth.SecuritySASLPlaintext
	SecuritySASLSSL       = auth.SecuritySASLSSL

	SASLPlain    = auth.SASLPlain
	SASLSCRAM256 = auth.SASLSCRAM256
	SASLSCRAM512 = auth.SASLSCRAM512
	SASLGSSAPI   = auth.SASLGSSAPI
	SASLOAuth    = auth.SASLOAuth
)
