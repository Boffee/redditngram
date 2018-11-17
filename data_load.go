package redditngram

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"github.com/boffee/zipio"
)

type Comment struct {
	Body        string `json:"body"`
	Subreddit   string `json:"subreddit"`
	SubredditId string `json:"subreddit_id"`
	CreatedUtc  int    `json:"created_utc"`
	Score       int    `json:"score"`
	ParentId    string `json:"parent_id"`
	LinkedId    string `json:"linked_id"`
}

func StreamRedditComments(year, month int) (<-chan *Comment, error) {
	datapath, err := GetRedditCommentsLocalPath(year, month)
	if err != nil {
		return nil, err
	}

	sender, err := zipio.ReadFromFileAuto(datapath)
	if err != nil {
		return nil, err
	}

	comments := make(chan *Comment)
	go func() {
		defer close(comments)
		var comment Comment
		lineCount := 0
		for bytes := range sender {
			json.Unmarshal(bytes, &comment)
			if comment.Body != "[deleted]" {
				comments <- &comment
			}
			lineCount++
			if lineCount%100000 == 0 {
				log.Printf("%d-%d line: %d\n", year, month, lineCount)
			}
		}

		log.Printf("%d-%d total lines processed: %d\n", year, month, lineCount)
	}()

	return comments, nil
}

func StreamRedditNgrams(year, month, order int) (<-chan string, error) {
	datapath, err := GetRedditNgramsLocalPath(year, month, order)
	if err != nil {
		return nil, err
	}

	ngramBytesStream, err := zipio.ReadFromFileAuto(datapath)
	if err != nil {
		return nil, err
	}

	ngramStrStream := make(chan string)
	go func() {
		defer close(ngramStrStream)
		for ngramBytes := range ngramBytesStream {
			ngramStrStream <- string(ngramBytes)
		}
	}()

	return ngramStrStream, nil
}

func LoadRedditNgramCounts(year, month, order int) (counts map[string]uint64, err error) {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		return nil, err
	}
	counts, err = LoadCountsFromTsv(datapath)
	return counts, err
}

func LoadCountsFromTsv(path string) (counts map[string]uint64, err error) {
	sender, err := zipio.ReadFromFileAuto(path)
	if err != nil {
		return nil, err
	}

	fields := make([]string, 2)
	counts = make(map[string]uint64)
	for bytes := range sender {
		fields = strings.Split(string(bytes), "\t")
		counts[fields[0]], err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
	}

	return counts, nil
}
