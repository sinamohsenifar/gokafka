package protocol

import "testing"

func TestCoordinatorRetriable(t *testing.T) {
	cases := []struct {
		code int16
		want bool
	}{
		{0, false},
		{ErrorCodeCoordinatorLoadInProgress, true},
		{ErrorCodeCoordinatorNotAvailable, true},
		{ErrorCodeNotCoordinator, true},
		{3, false},
	}
	for _, tc := range cases {
		if got := CoordinatorRetriable(tc.code); got != tc.want {
			t.Fatalf("code %d: got %v want %v", tc.code, got, tc.want)
		}
	}
}

func TestAPIErrorCode(t *testing.T) {
	err := apiError("join group", ErrorCodeNotCoordinator)
	code, ok := APIErrorCode(err)
	if !ok || code != ErrorCodeNotCoordinator {
		t.Fatalf("code=%d ok=%v", code, ok)
	}
}
