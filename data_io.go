package redditngram

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ulikunitz/xz"
)

const dateFormat = "2006-01"
const bz2Format = "RC_%04d-%02d.bz2"
const xzFormat = "RC_%04d-%02d.xz"
const ngramFormat = "RC_%04d-%02d_%dg.gz"

var DataStartDate, _ = time.Parse(dateFormat, "2005-12")
var XzStartDate, _ = time.Parse(dateFormat, "2017-12")

var RedditDataPath = getRedditDataPath()
var RedditCommentsPath = path.Join(RedditDataPath, "comments")
var RedditNgramsPath = path.Join(RedditDataPath, "ngrams")

type Comment struct {
	Body        string `json:"body"`
	Subreddit   string `json:"subreddit"`
	SubredditId string `json:"subreddit_id"`
	CreatedUtc  int    `json:"created_utc"`
	Score       int    `json:"score"`
	ParentId    string `json:"parent_id"`
	LinkedId    string `json:"linked_id"`
}

func LoadRedditCommentsJson(year, month int) (<-chan *Comment, error) {
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
	}()

	return comments, nil
}

func WriteRedditNgramCounts(counts map[string]uint64, year, month, order int) {
	datapath, err := GetRedditNgramsLocalPath(year, month, order)
	if err != nil {
		log.Fatalln(err)
	}
	err = os.MkdirAll(RedditNgramsPath, os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}

	fh, err := os.Create(datapath)
	if err != nil {
		log.Fatalln(err)
	}
	defer fh.Close()

	writer := gzip.NewWriter(fh)
	if err != nil {
		log.Fatalln(err)
	}
	defer writer.Close()

	const bufferSize = 4096
	var line string
	lineCount := 0
	for k, v := range counts {
		line = fmt.Sprintf("%s\t%d\n", k, v)
		_, err = writer.Write([]byte(line))
		if err != nil {
			log.Fatalln(err)
		}
		lineCount = (lineCount + 1) % bufferSize
		if lineCount == 0 {
			writer.Flush()
		}
	}
}

func LoadRedditNgramCounts(year, month, order int) (counts map[string]uint64, err error) {
	datapath, err := GetRedditNgramsLocalPath(year, month, order)
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
	reader, err := gzip.NewReader(fh)
	if err != nil {
		log.Fatalln(err)
	}
	counts, err = LoadCountsFromTsv(reader)
	if err != nil {
		log.Fatalln(err)
	}
	return counts, nil
}

func GetRedditCommentsLocalPath(year, month int) (datapath string, err error) {
	date, _ := time.Parse(dateFormat, fmt.Sprintf("%04d-%02d", year, month))
	if validateRedditCommentsDate(date) {
		filename := getRedditCommentsFilename(date)
		datapath = path.Join(RedditCommentsPath, filename)
	} else {
		return "", createDateOutOfRangeError(year, month)
	}
	return datapath, nil
}

func GetRedditNgramsLocalPath(year, month, order int) (datapath string, err error) {
	if month == 0 {
		if year < 2005 || year > time.Now().AddDate(0, -1, 0).Year() {
			return "", createDateOutOfRangeError(year, month)
		}
	} else {
		date, _ := time.Parse(dateFormat, fmt.Sprintf("%04d-%02d", year, month))
		if !validateRedditCommentsDate(date) {
			return "", createDateOutOfRangeError(year, month)
		}
	}
	filename := fmt.Sprintf(ngramFormat, year, month, order)
	datapath = path.Join(RedditNgramsPath, filename)
	return datapath, nil
}

func WriteCountsToTsv(counts map[string]uint64, w io.Writer) (err error) {
	var line string
	for k, v := range counts {
		line = fmt.Sprintf("%s\t%d\n", k, v)
		_, err = w.Write([]byte(line))
		if err != nil {
			return err
		}
	}
	return nil
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

func validateRedditCommentsDate(date time.Time) (is_valid bool) {
	dataEndDate := time.Now().AddDate(0, -1, 0)
	is_valid = true
	if date.Before(DataStartDate) || date.After(dataEndDate) {
		is_valid = false
	}
	return is_valid
}

func getRedditCommentsFilename(date time.Time) (filename string) {
	format := xzFormat
	if date.Before(XzStartDate) {
		format = bz2Format
	}
	filename = fmt.Sprintf(format, date.Year(), date.Month())
	return filename
}

func getRedditDataPath() string {
	RedditDataPath := "~/reddit"
	if datapath := os.Getenv("REDDIT_DATA"); datapath != "" {
		RedditDataPath = datapath
	}
	return RedditDataPath
}

func createDateOutOfRangeError(year, month int) error {
	return errors.New(fmt.Sprintf("date out of range: %04d-%02d", year, month))
}

func isDateOutOfRangeEorr(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "date out of range")
}
