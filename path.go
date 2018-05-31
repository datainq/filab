package filab

type Path interface {
	Join(p ...string) Path
	String() string
	Copy() Path
	Type() DriverType
}
