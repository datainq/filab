package filab

type Path interface {
	Join(p ...string) Path
	String() string
	Copy() Path
	Dir() Path
	Type() DriverType

	DirStr() string
	BaseStr() string
}
