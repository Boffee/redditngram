package redditngram

import (
	"fmt"
	"log"

	"github.com/boffee/zipio"
)

const newLineByte = byte('\n')

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

	err = zipio.WriteToFileAuto(sender, datapath)
	if err != nil {
		log.Fatalln(err)
	}
}

func WriteRedditNgramCountsHashed(counts *HashCounter, ngramVocab <-chan string, year, month, order int) {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		log.Fatalln(err)
	}

	sender := make(chan []byte)
	go func() {
		defer close(sender)
		var v uint64
		var k string
		var line string
		for k = range ngramVocab {
			counts.RLock()
			v = counts.Get([]byte(k))
			counts.RUnlock()
			line = fmt.Sprintf("%s\t%d\n", k, v)
			sender <- []byte(line)
		}
	}()

	err = zipio.WriteToFileAuto(sender, datapath)
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
			sender <- append([]byte(ngram), newLineByte)
		}
	}()

	err = zipio.WriteToFileAuto(sender, datapath)
	if err != nil {
		log.Fatalln(err)
	}
}
