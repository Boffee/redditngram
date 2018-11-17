package redditngram

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"log"
	"os"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

func ReadFromFileAuto(path string) (<-chan []byte, error) {
	comp, err := getCompressionType(path)
	if err != nil {
		if isUnsupportedCompressionError(err) {
			log.Println(err)
			log.Println("defaulting to raw file.")
			comp = Raw
		} else {
			return nil, err
		}
	}
	sender, err := ReadFromFile(path, comp)
	return sender, err
}

func ReadFromFile(path string, comp compression) (<-chan []byte, error) {
	var err error
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}

	var fh *os.File
	if fh, err = os.Open(path); err != nil {
		return nil, err
	}

	reader, deferFunc, err := newFileReader(fh, comp)
	deferFuncMerged := func() {
		defer func() {
			if err := fh.Close(); err != nil {
				log.Panic(err)
			}
		}()
		defer deferFunc()
	}
	sender := readFromReader(reader, deferFuncMerged)
	return sender, nil
}

func newFileReader(r io.Reader, comp compression) (nr io.Reader, deferFunc func(), err error) {
	switch comp {
	case Bz2:
		nr, deferFunc, err = newBz2Reader(r)
	case Gz:
		nr, deferFunc, err = newGzReader(r)
	case Lz4:
		nr, deferFunc, err = newLz4Reader(r)
	case Xz:
		nr, deferFunc, err = newXzReader(r)
	default:
		nr, deferFunc, err = newRawReader(r)
	}
	return nr, deferFunc, err
}

func newBz2Reader(r io.Reader) (io.Reader, func(), error) {
	bz2r := bzip2.NewReader(r)
	return bz2r, func() {}, nil
}

func newGzReader(r io.Reader) (io.Reader, func(), error) {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		log.Panic(err)
	}
	deferFunc := func() {
		if err := gzr.Close(); err != nil {
			log.Panic(err)
		}
	}
	return gzr, deferFunc, nil
}

func newLz4Reader(r io.Reader) (io.Reader, func(), error) {
	lz4r := lz4.NewReader(r)
	return lz4r, func() {}, nil
}

func newXzReader(r io.Reader) (io.Reader, func(), error) {
	xzr, err := xz.NewReader(r)
	if err != nil {
		log.Panic(err)
	}
	return xzr, func() {}, nil
}

func newRawReader(r io.Reader) (io.Reader, func(), error) {
	rawr := bufio.NewReader(r)
	return rawr, func() {}, nil
}

func readFromReader(r io.Reader, deferFunc func()) <-chan []byte {
	sender := make(chan []byte)
	go func() {
		if deferFunc != nil {
			defer deferFunc()
		}
		scanner := bufio.NewScanner(r)
		defer close(sender)
		for scanner.Scan() {
			sender <- scanner.Bytes()
		}
	}()
	return sender
}
