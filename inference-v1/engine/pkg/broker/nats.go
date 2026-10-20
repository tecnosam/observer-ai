package broker

import (
	"github.com/nats-io/nats.go"
)

// NATSAuth is how the engine proves who it is to NATS. Set at most one of
// a credentials file, an nkey seed file, a user with a password, or a
// token. All empty connects without authentication (or with credentials in
// the URL).
type NATSAuth struct {
	CredentialsFile string // a .creds file: a user JWT and its nkey seed
	NkeySeedFile    string // a file holding an nkey seed (SU...)
	User            string
	Password        string
	Token           string
}

// NATSTLS is the TLS the connection uses. CAFile verifies the server;
// CertFile and KeyFile, together, are a client certificate for mutual TLS.
// Any of them set turns TLS on, as a tls:// URL also does.
type NATSTLS struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// natsOptions turns the settings into NATS client options.
func natsOptions(auth NATSAuth, tls NATSTLS) ([]nats.Option, error) {
	opts, err := authOptions(auth)
	if err != nil {
		return nil, err
	}
	return append(opts, tlsOptions(tls)...), nil
}

func authOptions(a NATSAuth) ([]nats.Option, error) {
	switch {
	case a.CredentialsFile != "":
		return []nats.Option{nats.UserCredentials(a.CredentialsFile)}, nil
	case a.NkeySeedFile != "":
		opt, err := nats.NkeyOptionFromSeed(a.NkeySeedFile)
		if err != nil {
			return nil, err
		}
		return []nats.Option{opt}, nil
	case a.User != "":
		return []nats.Option{nats.UserInfo(a.User, a.Password)}, nil
	case a.Token != "":
		return []nats.Option{nats.Token(a.Token)}, nil
	}
	return nil, nil
}

func tlsOptions(t NATSTLS) []nats.Option {
	var opts []nats.Option
	if t.CAFile != "" {
		opts = append(opts, nats.RootCAs(t.CAFile))
	}
	if t.CertFile != "" {
		opts = append(opts, nats.ClientCert(t.CertFile, t.KeyFile))
	}
	return opts
}
