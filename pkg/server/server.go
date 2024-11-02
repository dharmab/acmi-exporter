package server

type Telemetry struct {
	Password string
}

func New(password string) *Telemetry {
	return &Telemetry{
		Password: password,
	}
}
