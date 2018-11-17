package redditngram

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dsnet/compress/bzip2"
	"github.com/ulikunitz/xz"
)

func WriteToFileAuto(sender <-chan []byte, path string) (err error) {
	comp, err := getCompressionType(path)
	if err != nil {
		if isUnsupportedCompressionError(err) {
			comp = Raw
		} else {
			return err
		}
	}
	err = WriteToFile(sender, path, comp)
	return err
}

func WriteToFile(sender <-chan []byte, path string, comp compression) (err error) {
	dirPath := filepath.Dir(path)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(path); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()

	writer, deferFunc, err := newWriter(fh, comp)
	if err != nil {
		return err
	}
	defer deferFunc()

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
}

func newWriter(w io.Writer, comp compression) (nw io.Writer, deferFunc func(), err error) {
	switch comp {
	case Bz2:
		nw, deferFunc, err = newBz2Writer(w, 1)
	case Gz:
		nw, deferFunc, err = newGzWriter(w)
	case Lz4:
		nw, deferFunc, err = newLz4Writer(w)
	case Xz:
		nw, deferFunc, err = newXzWriter(w)
	default:
		nw, deferFunc, err = newRawWriter(w)
	}
	return nw, deferFunc, err
}

func newBz2Writer(w io.Writer, level int) (io.Writer, func(), error) {
	bz2w, err := bzip2.NewWriter(w, &bzip2.WriterConfig{Level: level})
	if err != nil {
		return nil, nil, err
	}
	deferFunc := func() {
		if err := bz2w.Close(); err != nil {
			log.Panic(err)
		}
	}
	return bz2w, deferFunc, err
}

func newGzWriter(w io.Writer) (io.Writer, func(), error) {
	gzw := gzip.NewWriter(w)
	deferFunc := func() {
		if err := gzw.Close(); err != nil {
			log.Panic(err)
		}
	}
	return gzw, deferFunc, nil
}

func newLz4Writer(w io.Writer) (io.Writer, func(), error) {
	lz4w := gzip.NewWriter(w)
	deferFunc := func() {
		if err := lz4w.Close(); err != nil {
			log.Panic(err)
		}
	}
	return lz4w, deferFunc, nil
}

func newXzWriter(w io.Writer) (io.Writer, func(), error) {
	xzw, err := xz.NewWriter(w)
	if err != nil {
		return nil, nil, err
	}
	deferFunc := func() {
		if err := xzw.Close(); err != nil {
			log.Panic(err)
		}
	}
	return xzw, deferFunc, nil
}

func newRawWriter(w io.Writer) (io.Writer, func(), error) {
	fw := bufio.NewWriter(w)
	deferFunc := func() {}
	return fw, deferFunc, nil
}

func writeToWriter(
	w io.Writer, sender <-chan []byte) (err error) {
	for bytes := range sender {
		_, err = w.Write(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}
