package redditngram

import (
	"fmt"
	"path/filepath"
	"strings"
)

type compression string

const (
	Bz2 compression = "bz2"
	Gz  compression = "gz"
	Lz4 compression = "Lz4"
	Xz  compression = "xz"
	Raw compression = "raw"
)

func getCompressionType(path string) (comp compression, err error) {
	switch filepath.Ext(path) {
	case ".bz2":
		comp = Bz2
	case ".gz":
		comp = Gz
	case ".lz4":
		comp = Lz4
	case ".xz":
		comp = Xz
	case ".txt":
		comp = Raw
	default:
		err = fmt.Errorf("unsupported compression type: %s\n"+
			"only bz2, gz, lz4, xz, and txt are supported", path)
		return "", err
	}
	return comp, nil
}

func isUnsupportedCompressionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "unsupported compression type")
}
