package identity

import (
	"errors"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// Credential + identity vectors: executed directly from
// spec/vectors/identity_vectors.yaml (the spec is the source of truth;
// no hand-mirrored copies).
func TestParse_ConformanceVectors(t *testing.T) {
	vectors := loadIdentityVectors(t)
	if len(vectors) == 0 {
		t.Fatal("no vectors loaded — spec/vectors/identity_vectors.yaml missing?")
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			switch v.Entry {
			case "http-forward":
				id, err := Parse(v.Credential)
				if err != nil {
					t.Fatalf("Parse(%q): %v", v.Credential, err)
				}
				// spec/identity.md §2: 空 platform 表示"用 Default" —
				// the vector keeps the raw split, Parse normalizes.
				wantPlatform := v.Expect.Platform
				if wantPlatform == "" {
					wantPlatform = DefaultPlatformID
				}
				if id.Token != v.Expect.Token || id.PlatformID != wantPlatform || id.Account != v.Expect.Account {
					t.Fatalf("Parse(%q) = (%q, %q, %q), want (%q, %q, %q)",
						v.Credential, id.Token, id.PlatformID, id.Account,
						v.Expect.Token, wantPlatform, v.Expect.Account)
				}
			case "identity-only":
				platform, account := SplitIdentity(v.Identity)
				if platform != v.Expect.Platform || account != v.Expect.Account {
					t.Fatalf("SplitIdentity(%q) = (%q, %q), want (%q, %q)",
						v.Identity, platform, account, v.Expect.Platform, v.Expect.Account)
				}
			case "socks5":
				// RFC1929: username 整串即身份（无最右切分），password 即 Token。
				platform, account := SplitIdentity(v.Username)
				wantPlatform := v.Expect.Platform
				if wantPlatform == "" {
					wantPlatform = DefaultPlatformID
				}
				if platform != wantPlatform || account != v.Expect.Account || v.Password != v.Expect.Token {
					t.Fatalf("socks5 (%q, %q) split = (%q, %q, %q), want (%q, %q, %q)",
						v.Username, v.Password, platform, account, v.Password,
						wantPlatform, v.Expect.Account, v.Expect.Token)
				}
			case "reverse":
				// Executed by the reverse adapter's own YAML consumer
				// (internal/adapters/reverse): the path grammar lives
				// there, not in the identity kernel.
			default:
				t.Fatalf("unknown vector entry %q — extend the executor", v.Entry)
			}
		})
	}
}

// --- yaml loader (spec/vectors is the normative conformance source) ---

type identityVectorFile struct {
	Version int              `yaml:"version"`
	Vectors []identityVector `yaml:"vectors"`
}

type identityVector struct {
	Name       string `yaml:"name"`
	Entry      string `yaml:"entry"`
	Credential string `yaml:"credential"`
	Identity   string `yaml:"identity"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	Expect     struct {
		Token    string `yaml:"token"`
		Platform string `yaml:"platform"`
		Account  string `yaml:"account"`
	} `yaml:"expect"`
}

func loadIdentityVectors(t *testing.T) []identityVector {
	t.Helper()
	raw, err := os.ReadFile("../../../spec/vectors/identity_vectors.yaml")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var f identityVectorFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	seen := map[string]bool{}
	for _, v := range f.Vectors {
		if seen[v.Name] {
			t.Fatalf("duplicate vector name %q", v.Name)
		}
		seen[v.Name] = true
	}
	return f.Vectors
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
	valid := []string{"egress-123456", "abcDEF019_-", "x", ""}
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
