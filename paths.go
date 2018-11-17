package redditngram

import (
	"fmt"
	"os"
	"path"
	"time"
)

const dateFormat = "2006-01"
const bz2Format = "RC_%04d-%02d.bz2"
const xzFormat = "RC_%04d-%02d.xz"
const ngramFormat = "RC_%04d-%02d_%dg.lz4"
const ngramCountFormat = "RC_%04d-%02d_%dgc.lz4"

var RedditDataPath = getRedditDataPath()
var RedditCommentsPath = path.Join(RedditDataPath, "comments")
var RedditNgramsPath = path.Join(RedditDataPath, "ngrams")

func GetRedditCommentsLocalPath(year, month int) (datapath string, err error) {
	if isValidRedditCommentsDate(year, month) {
		filename := getRedditCommentsFilename(month, year)
		datapath = path.Join(RedditCommentsPath, filename)
	} else {
		return "", createDateOutOfRangeError(year, month)
	}
	return datapath, nil
}

func GetRedditNgramsLocalPath(year, month, order int) (datapath string, err error) {
	if isValidRedditNgramsDate(year, month) {
		filename := fmt.Sprintf(ngramFormat, year, month, order)
		datapath = path.Join(RedditNgramsPath, filename)
	} else {
		return "", createDateOutOfRangeError(year, month)
	}
	return datapath, nil
}

func GetRedditNgramCountsLocalPath(year, month, order int) (datapath string, err error) {
	if isValidRedditNgramsDate(year, month) {
		filename := fmt.Sprintf(ngramCountFormat, year, month, order)
		datapath = path.Join(RedditNgramsPath, filename)
	} else {
		return "", createDateOutOfRangeError(year, month)
	}
	return datapath, nil
}

func getRedditCommentsFilename(month, year int) (filename string) {
	date, _ := time.Parse(dateFormat, fmt.Sprintf("%04d-%02d", year, month))
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

func redditUptoNgramCountsCacheExists(year, month, order int) (cacheExists bool, err error) {
	for i := 0; i < order; i++ {
		cacheExists, err = redditNgramCountsCacheExists(year, month, order)
		if err != nil || !cacheExists {
			return cacheExists, err
		}
	}
	return cacheExists, nil
}

func redditNgramCountsCacheExists(year, month, order int) (cacheExists bool, err error) {
	datapath, err := GetRedditNgramCountsLocalPath(year, month, order)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(datapath); os.IsNotExist(err) {
		return false, nil
	}
	return true, nil
}
