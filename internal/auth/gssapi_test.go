package auth

import (
	"context"
	"testing"
)

func TestGSSAPINoTokenSource(t *testing.T) {
	err := gssapi(context.Background(), nil, Config{
		SASL: SASLConfig{
			Kerberos: KerberosConfig{Principal: "kafka/client@REALM"},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}