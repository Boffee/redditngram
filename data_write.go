package redditngram

import (
	"fmt"
	"log"
)

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

	err = WriteToFileAuto(sender, datapath)
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

	err = WriteToFileAuto(sender, datapath)
	if err != nil {
		log.Fatalln(err)
	}
}
