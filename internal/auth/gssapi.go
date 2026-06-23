package auth

import (
	"context"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func gssapi(ctx context.Context, conn requester, sec Config) error {
	k := sec.SASL.Kerberos
	if k.TokenProvider == nil && len(k.InitToken) == 0 {
		return fmt.Errorf("auth: GSSAPI requires InitToken or TokenProvider")
	}
	token := append([]byte(nil), k.InitToken...)
	for round := 0; round < 32; round++ {
		resp, err := conn.Request(ctx, protocol.APISaslAuthenticate, protocol.VerSaslAuthenticate, wrapSasl(token))
		if err != nil {
			return err
		}
		challenge, complete, err := parseGSSAPIResponse(resp)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		if k.TokenProvider == nil {
			return fmt.Errorf("auth: GSSAPI challenge received but KerberosConfig.TokenProvider is nil")
		}
		token, err = k.TokenProvider(ctx, challenge)
		if err != nil {
			return fmt.Errorf("auth: GSSAPI token provider: %w", err)
		}
	}
	return fmt.Errorf("auth: GSSAPI handshake exceeded round limit")
}

func parseGSSAPIResponse(raw []byte) (challenge []byte, complete bool, err error) {
	msg, err := parseAuthBytes(raw)
	if err != nil {
		return nil, false, err
	}
	if msg == "" {
		return nil, true, nil
	}
	return []byte(msg), false, nil
}
