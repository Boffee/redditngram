package redditngram

import (
	"fmt"
	"log"
	"os"

	"github.com/boffee/zipio"
)

const newLineByte = byte('\n')

func WriteRedditNgramCounts(ngramCounts map[string]uint64, year, month, order int) error {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		return err
	}

	sender := make(chan []byte)
	go func() {
		defer close(sender)
		var line string
		for k, v := range ngramCounts {
			line = fmt.Sprintf("%s\t%d\n", k, v)
			sender <- []byte(line)
		}
	}()

	err = zipio.WriteToFileAuto(sender, datapath)
	if err != nil {
		return err
	}

	return nil
}

func WriteRedditNgramCountsHashed(ngramHCounts *HashCounter, ngramVocab <-chan string, year, month, order int) error {
	ngramCachePath, err := GetRedditNgramsLocalPath(year, month, order)
	if err != nil {
		return err
	}

	// Stream n-gram vocab to disk and count using hashed n-grams to save memory.
	err = WriteRedditNgrams(ngramVocab, year, month, order)
	if err != nil {
		return err
	}
	// Delete n-gram vocab after n-gram vocab count write is finished.
	defer func() {
		err = os.Remove(ngramCachePath)
		if err != nil {
			log.Panic(err)
		}
	}()

	// Create n-gram vocab count file by merging n-gram vocab read from disk
	// and hashed n-gram counts from memory.
	ngramVocab, err = StreamRedditNgramVocab(year, month, order)
	sender := make(chan []byte)
	go func() {
		defer close(sender)
		var v uint64
		var k string
		var line string
		for k = range ngramVocab {
			ngramHCounts.RLock()
			v = ngramHCounts.Get([]byte(k))
			ngramHCounts.RUnlock()
			line = fmt.Sprintf("%s\t%d\n", k, v)
			sender <- []byte(line)
		}
	}()

	err = zipio.WriteToFileAuto(sender, datapath)
	if err != nil {
		return err
	}

	return nil
}

func WriteRedditNgrams(ngrams <-chan string, year, month, order int) error {
	datapath, err := GetRedditNgramsLocalPath(year, month, order)
	if err != nil {
		return err
	}

	sender := make(chan []byte)
	go func() {
		defer close(sender)
		for ngram := range ngrams {
			sender <- append([]byte(ngram), newLineByte)
		}
	}()

	err = zipio.WriteToFileAuto(sender, datapath)
	if err != nil {
		return err
	}

	return nil
}
