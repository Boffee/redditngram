package redditngram

import (
	"fmt"
	"strings"
	"time"
)

var DataStartDate, _ = time.Parse(dateFormat, "2005-12")
var XzStartDate, _ = time.Parse(dateFormat, "2017-12")

func isValidRedditCommentsDate(year, month int) bool {
	date, _ := time.Parse(dateFormat, fmt.Sprintf("%04d-%02d", year, month))
	dataEndDate := time.Now().AddDate(0, -1, 0)
	if date.Before(DataStartDate) || date.After(dataEndDate) {
		return false
	}
	return true
}

func isValidRedditNgramsDate(year, month int) bool {
	if month == 0 {
		if year < 2005 || year > time.Now().AddDate(0, -1, 0).Year() {
			return false
		}
	} else {
		if !isValidRedditCommentsDate(year, month) {
			return false
		}
	}
	return true
}

func createDateOutOfRangeError(year, month int) error {
	return fmt.Errorf("date out of range: %04d-%02d", year, month)
}

func isDateOutOfRangeError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "date out of range")
}
