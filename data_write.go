package redditngram

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

type compression string

func WriteRedditNgramCounts(counts map[string]uint64, year, month, order int) {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		log.Fatalln(err)
	}

	sender := make(chan []byte)
	go func() {
		defer close(sender)
		var line string
		for k, v := range counts {
			line = fmt.Sprintf("%s\t%d\n", k, v)
			sender <- []byte(line)
		}
	}()

	err = WriteToAuto(sender, datapath)
	if err != nil {
		log.Fatalln(err)
	}
}

func WriteRedditNgrams(ngrams <-chan string, year, month, order int) {
	datapath, err := GetRedditNgramsLocalPath(year, month, order)
	if err != nil {
		log.Fatalln(err)
	}

	sender := make(chan []byte)
	go func() {
		defer close(sender)
		for ngram := range ngrams {
			sender <- []byte(ngram)
		}
	}()

	err = WriteToAuto(sender, datapath)
	if err != nil {
		log.Fatalln(err)
	}
}

func WriteToAuto(sender <-chan []byte, path string) (err error) {
	switch filepath.Ext(path) {
	case ".bz2":
		err = WriteToBz2(sender, path, 1)
	case ".gz":
		err = WriteToGz(sender, path)
	case ".lz4":
		err = WriteToLz4(sender, path)
	case ".xz":
		err = WriteToXz(sender, path)
	case ".txt":
		err = WriteToFile(sender, path)
	default:
		log.Println("Only bz2, gz, lz4, xz, txt are support. Given: ", path)
		log.Println("Defaulting to txt.")
		err = WriteToFile(sender, path)
	}
	return err
}

func WriteToFile(sender <-chan []byte, txtPath string) (err error) {
	dirPath := filepath.Dir(txtPath)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(txtPath); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()
	writer := bufio.NewWriter(fh)

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
}

func WriteToBz2(sender <-chan []byte, bz2Path string, level int) (err error) {
	dirPath := filepath.Dir(bz2Path)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(bz2Path); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()

	var writer *bzip2.Writer
	if writer, err = bzip2.NewWriter(fh, &bzip2.WriterConfig{Level: level}); err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
}

func WriteToGz(sender <-chan []byte, gzipPath string) (err error) {
	dirPath := filepath.Dir(gzipPath)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(gzipPath); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()

	writer := gzip.NewWriter(fh)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
}

func WriteToLz4(sender <-chan []byte, lz4Path string) (err error) {
	dirPath := filepath.Dir(lz4Path)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(lz4Path); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()

	writer := lz4.NewWriter(fh)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
}

func WriteToXz(sender <-chan []byte, xzPath string) (err error) {
	dirPath := filepath.Dir(xzPath)
	if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}

	var fh *os.File
	if fh, err = os.Create(xzPath); err != nil {
		return err
	}
	defer func() {
		if err := fh.Close(); err != nil {
			log.Panic(err)
		}
	}()

	var writer *xz.Writer
	if writer, err = xz.NewWriter(fh); err != nil {
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	if err = writeToWriter(writer, sender); err != nil {
		return err
	}

	return nil
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
