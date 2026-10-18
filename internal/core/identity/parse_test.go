package identity

import (
	"errors"
	"testing"
)

// Credential-level vectors: subset of spec/vectors/identity_vectors.yaml
// (entry: http-forward). Platform "" means "use Default" at a higher
// layer; Parse itself reports the raw split.
func TestParse_CredentialVectors(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		platform string
		account  string
		token    string
	}{
		{"forward_basic_full", "Nimbus.Tom:resin-123456", "Nimbus", "Tom", "resin-123456"},
		{"forward_basic_empty_platform", ".Tom:resin-123456", "", "Tom", "resin-123456"},
		{"forward_basic_empty_account", "Nimbus:resin-123456", "Nimbus", "", "resin-123456"},
		{"forward_basic_trailing_dot", "Nimbus.:resin-123456", "Nimbus", "", "resin-123456"},
		{"forward_basic_account_with_colon", "MyHub.bEA:234:resin-123456", "MyHub", "bEA:234", "resin-123456"},
		{"forward_credential_empty", "", "", "", ""},
		{"forward_credential_bare_platform", "a", "a", "", ""},
		{"forward_credential_lone_colon", ":", "", "", ""},
		{"forward_credential_dot_colon", ".:", "", "", ""},
		{"forward_credential_colon_dot", ":.", "", "", "."},
		{"forward_credential_dot_b_colon", ".b:", "", "b", ""},
		{"forward_credential_dot_colon_c", ".:c", "", "", "c"},
		{"forward_credential_double_colon", ".::c", "", ":", "c"},
		{"forward_credential_double_dot", "..:", "", ".", ""},
		{"forward_credential_a_dot_b_colon_c", "a.b:c", "a", "b", "c"},
		// first '.' wins over a later ':'
		{"dot_before_colon", "MyHub.ops:prod:tok", "MyHub", "ops:prod", "tok"},
		// first ':' wins over a later '.'
		{"colon_before_dot", "MyHub:ops.prod:tok", "MyHub", "ops.prod", "tok"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.raw)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.raw, err)
			}
			// Empty platform is normalized to Default by Parse.
			wantPlatform := tc.platform
			if wantPlatform == "" {
				wantPlatform = DefaultPlatformID
			}
			if got.PlatformID != wantPlatform || got.Account != tc.account || got.Token != tc.token {
				t.Fatalf("Parse(%q) = %+v, want platform=%q account=%q token=%q",
					tc.raw, got, wantPlatform, tc.account, tc.token)
			}
			if got.Random != (tc.account == "") {
				t.Fatalf("Parse(%q).Random = %v, want %v", tc.raw, got.Random, tc.account == "")
			}
		})
	}
}

// Identity-only vectors (entry: identity-only): the bare identity split
// used by the reverse-proxy path after path-segment decoding.
func TestSplitIdentity_Vectors(t *testing.T) {
	tests := []struct {
		name     string
		identity string
		platform string
		account  string
	}{
		{"identity_empty", "", "", ""},
		{"identity_lone_colon", ":", "", ""},
		{"identity_lone_dot", ".", "", ""},
		{"identity_bare", "a", "a", ""},
		{"identity_trailing_colon", "a:", "a", ""},
		{"identity_leading_colon", ":b", "", "b"},
		{"identity_dot_colon", ".:", "", ":"},
		{"identity_colon_dot", ":.", "", "."},
		{"identity_colon_dotdot", ":..", "", ".."},
		{"identity_colon_split", "a:b", "a", "b"},
		{"identity_first_dot_wins_over_later_colon", "MyHub.ops:prod", "MyHub", "ops:prod"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			platform, account := SplitIdentity(tc.identity)
			if platform != tc.platform || account != tc.account {
				t.Fatalf("SplitIdentity(%q) = (%q, %q), want (%q, %q)",
					tc.identity, platform, account, tc.platform, tc.account)
			}
		})
	}
}

func TestParse_EmptyAccountMarksRandom(t *testing.T) {
	for _, raw := range []string{"Nimbus:tok", ":tok", "Nimbus.:tok", ".:"} {
		id, err := Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !id.Random {
			t.Fatalf("Parse(%q).Random = false, want true (no lease must be built)", raw)
		}
	}
	id, _ := Parse("Nimbus.Tom:tok")
	if id.Random {
		t.Fatal("Parse with account must not set Random")
	}
	if id.PlatformID != "Nimbus" || id.Account != "Tom" || id.Token != "tok" {
		t.Fatalf("unexpected identity: %+v", id)
	}
}

func TestValidateToken(t *testing.T) {
	valid := []string{"resin-123456", "abcDEF019_-", "x", ""}
	for _, tok := range valid {
		if err := ValidateToken(tok); err != nil {
			t.Fatalf("ValidateToken(%q) = %v, want nil", tok, err)
		}
	}

	forbidden := []string{
		"a.b", "a:b", "a|b", "a/b", `a\b`, "a@b", "a?b", "a#b", "a%b", "a~b",
		"a b", "a\tb", "a\nb", "a\rb", "a　b", // incl. full-width space
	}
	for _, tok := range forbidden {
		if err := ValidateToken(tok); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("ValidateToken(%q) = %v, want ErrInvalidToken", tok, err)
		}
	}

	for _, tok := range []string{"api", "healthz", "ui"} {
		if err := ValidateToken(tok); !errors.Is(err, ErrReservedToken) {
			t.Fatalf("ValidateToken(%q) = %v, want ErrReservedToken", tok, err)
		}
	}
	// Near-misses of reserved values are fine.
	for _, tok := range []string{"apix", "myapi", "UI"} {
		if err := ValidateToken(tok); err != nil {
			t.Fatalf("ValidateToken(%q) = %v, want nil", tok, err)
		}
	}
}

func TestParse_DoesNotValidateToken(t *testing.T) {
	// Conformance: token "." must parse successfully (validation is a
	// separate step via ValidateToken, enforced at auth/config time).
	id, err := Parse(":.")
	if err != nil {
		t.Fatal(err)
	}
	if id.Token != "." {
		t.Fatalf("Parse(%q).Token = %q, want %q", ":.", id.Token, ".")
	}
	if err := ValidateToken(id.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("ValidateToken(%q) = %v, want ErrInvalidToken", id.Token, err)
	}
}
