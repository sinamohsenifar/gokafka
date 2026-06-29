package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ApiVersion describes a supported API key/version pair.
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// EncodeApiVersionsRequest builds an ApiVersions request body.
func EncodeApiVersionsRequest(softwareName, softwareVersion string) []byte {
	if VerApiVersions >= 3 {
		buf := wire.NewBuffer(32)
		if softwareName == "" {
			softwareName = "gokafka"
		}
		buf.WriteCompactString(softwareName)
		buf.WriteCompactString(softwareVersion)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	return nil
}

// DecodeApiVersionsResponse parses broker API version ranges.
func DecodeApiVersionsResponse(version int16, body []byte) ([]ApiVersion, int16, error) {
	if version >= 3 {
		return decodeApiVersionsResponseFlex(body)
	}
	return decodeApiVersionsResponseLegacy(version, body)
}

func decodeApiVersionsResponseLegacy(version int16, body []byte) ([]ApiVersion, int16, error) {
	buf := wire.FromBytes(body)
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, 0, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, errCode, err
	}
	out := make([]ApiVersion, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		key, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		min, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		max, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		out = append(out, ApiVersion{APIKey: key, MinVersion: min, MaxVersion: max})
	}
	if version >= 2 {
		if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
			return nil, errCode, err
		}
	}
	return out, errCode, nil
}

func decodeApiVersionsResponseFlex(body []byte) ([]ApiVersion, int16, error) {
	buf := wire.FromBytes(body)
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, 0, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, errCode, err
	}
	out := make([]ApiVersion, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		key, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		min, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		max, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		out = append(out, ApiVersion{APIKey: key, MinVersion: min, MaxVersion: max})
		if err := buf.SkipTagSection(); err != nil {
			return nil, errCode, err
		}
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, errCode, err
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, errCode, err
	}
	return out, errCode, nil
}

// NegotiateVersion picks the highest mutually supported version.
func NegotiateVersion(versions []ApiVersion, apiKey, clientMax int16) int16 {
	if clientMax <= 0 {
		return 0
	}
	for _, v := range versions {
		if v.APIKey != apiKey {
			continue
		}
		if clientMax < v.MinVersion {
			return 0
		}
		if clientMax > v.MaxVersion {
			return v.MaxVersion
		}
		return clientMax
	}
	return clientMax
}
