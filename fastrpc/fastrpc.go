package fastrpc

import (
	"io"
)

type Serializable interface {
	Marshal(io.Writer)
	Unmarshal(io.Reader, int) error
	New() Serializable
	Size() int
}
