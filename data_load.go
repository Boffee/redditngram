package redditngram

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
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

func LoadRedditComments(year, month int) (<-chan *Comment, error) {
	datapath, err := GetRedditCommentsLocalPath(year, month)
	if err != nil {
		return nil, err
	} else if _, err := os.Stat(datapath); os.IsNotExist(err) {
		return nil, err
	}

	comments := make(chan *Comment)
	go func() {
		defer close(comments)
		fh, err := os.Open(datapath)
		if err != nil {
			log.Fatalln(err)
		}
		defer fh.Close()

		var reader io.Reader
		switch filepath.Ext(datapath) {
		case ".xz":
			reader, err = xz.NewReader(fh)
			if err != nil {
				log.Fatalln(err)
			}
		case ".bz2":
			reader = bzip2.NewReader(fh)
		default:
			log.Panic("Only .xz and .bz2 are support. Given: ", datapath)
		}

		scanner := bufio.NewScanner(reader)
		var comment Comment
		lineCount := 0
		for scanner.Scan() {
			json.Unmarshal([]byte(scanner.Text()), &comment)
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

func LoadRedditNgramCounts(year, month, order int) (counts map[string]uint64, err error) {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		return nil, err
	} else if _, err := os.Stat(datapath); os.IsNotExist(err) {
		return nil, err
	}
	fh, err := os.Open(datapath)
	if err != nil {
		log.Fatalln(err)
	}
	defer fh.Close()
	reader := lz4.NewReader(fh)
	if err != nil {
		log.Fatalln(err)
	}
	counts, err = LoadCountsFromTsv(reader)
	if err != nil {
		log.Fatalln(err)
	}
	return counts, nil
}

func LoadCountsFromTsv(r io.Reader) (counts map[string]uint64, err error) {
	scanner := bufio.NewScanner(r)
	fields := make([]string, 2)
	counts = make(map[string]uint64)
	for scanner.Scan() {
		fields = strings.Split(scanner.Text(), "\t")
		counts[fields[0]], err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return counts, nil
}
