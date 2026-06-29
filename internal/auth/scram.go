package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"strings"

	"github.com/sinamohsenifar/gokafka/internal/limits"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func scramSHA256(ctx context.Context, conn requester, sec Config) error {
	return scramExchange(ctx, conn, sec, sha256.New, "SCRAM-SHA-256")
}

func scramSHA512(ctx context.Context, conn requester, sec Config) error {
	return scramExchange(ctx, conn, sec, sha512.New, "SCRAM-SHA-512")
}

func scramExchange(ctx context.Context, conn requester, sec Config, newHash func() hash.Hash, _ string) error {
	nonce := randomNonce()
	clientFirst := fmt.Sprintf("n,,n=%s,r=%s", saslEscape(sec.SASL.Username), nonce)
	serverFirst, err := saslRound(ctx, conn, clientFirst)
	if err != nil {
		return err
	}
	fields := parseScramFields(serverFirst)
	serverNonce := fields["r"]
	if !strings.HasPrefix(serverNonce, nonce) {
		return fmt.Errorf("auth: server nonce mismatch (client=%q server=%q msg=%q)", nonce, serverNonce, serverFirst)
	}
	salt, err := base64.StdEncoding.DecodeString(fields["s"])
	if err != nil {
		return err
	}
	iterations := 4096
	fmt.Sscanf(fields["i"], "%d", &iterations)
	if iterations < 1 {
		iterations = 4096
	}
	if iterations > limits.MaxSCRAMIterations() {
		return fmt.Errorf("auth: SCRAM iteration count %d exceeds limit %d", iterations, limits.MaxSCRAMIterations())
	}

	salted := pbkdf2(newHash, sec.SASL.Password, salt, iterations)
	clientKey := hmacSum(newHash, salted, []byte("Client Key"))
	storedKey := hashSum(newHash, clientKey)
	clientFirstBare := fmt.Sprintf("n=%s,r=%s", saslEscape(sec.SASL.Username), nonce)
	clientFinalWithoutProof := fmt.Sprintf("c=biws,r=%s", serverNonce)
	authMsg := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	proof := xor(clientKey, hmacSum(newHash, storedKey, []byte(authMsg)))
	clientFinal := fmt.Sprintf("c=biws,r=%s,p=%s", serverNonce, base64.StdEncoding.EncodeToString(proof))
	_, err = saslRound(ctx, conn, clientFinal)
	return err
}

func saslRound(ctx context.Context, conn requester, msg string) (string, error) {
	resp, err := conn.Request(ctx, protocol.APISaslAuthenticate, protocol.VerSaslAuthenticate, wrapSasl([]byte(msg)))
	if err != nil {
		return "", err
	}
	return parseAuthBytes(resp)
}

func parseAuthBytes(raw []byte) (string, error) {
	body, err := protocol.ResponseBody(raw)
	if err != nil {
		return "", err
	}
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return "", err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return "", err
	}
	errMsg, err := buf.ReadString() // SCRAM server-first may appear here when auth_bytes is empty
	if err != nil {
		return "", err
	}
	authBytes, err := buf.ReadBytes()
	if err != nil {
		return "", err
	}
	if code != 0 {
		msg := string(authBytes)
		if msg == "" {
			msg = errMsg
		}
		return "", fmt.Errorf("auth: sasl failed code %d: %s", code, msg)
	}
	if len(authBytes) == 0 && errMsg != "" {
		authBytes = []byte(errMsg)
	}
	if code == 0 && len(authBytes) == 0 && errMsg == "" {
		return "", nil // SCRAM authentication complete
	}
	if len(authBytes) == 0 {
		return "", fmt.Errorf("auth: empty sasl response")
	}
	return string(authBytes), nil
}

func parseScramFields(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if i := strings.IndexByte(part, '='); i > 0 {
			out[part[:i]] = part[i+1:]
		}
	}
	return out
}

func pbkdf2(hf func() hash.Hash, password string, salt []byte, iter int) []byte {
	size := hf().Size()
	out := make([]byte, size)
	buf := append(append([]byte(nil), salt...), 0, 0, 0, 1)
	u := hmacSum(hf, []byte(password), buf)
	copy(out, u)
	for i := 1; i < iter; i++ {
		u = hmacSum(hf, []byte(password), u)
		for j := 0; j < size; j++ {
			out[j] ^= u[j]
		}
	}
	return out
}

func hashSum(hf func() hash.Hash, b []byte) []byte {
	h := hf()
	h.Write(b)
	return h.Sum(nil)
}

func hmacSum(hf func() hash.Hash, key, msg []byte) []byte {
	m := hmac.New(hf, key)
	m.Write(msg)
	return m.Sum(nil)
}

func xor(a, b []byte) []byte {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = a[i] ^ b[i]
	}
	return out
}

func randomNonce() string {
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func saslEscape(s string) string {
	return strings.NewReplacer("=", "=3D", ",", "=2C").Replace(s)
}

func wrapSasl(b []byte) []byte {
	out := make([]byte, 4+len(b))
	out[0] = byte(len(b) >> 24)
	out[1] = byte(len(b) >> 16)
	out[2] = byte(len(b) >> 8)
	out[3] = byte(len(b))
	copy(out[4:], b)
	return out
}
