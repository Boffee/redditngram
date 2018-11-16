package redditngram

import (
	"log"
	"os"
	"strings"
	"sync"
)

const MaxRedditTokenLength = 25

func GenerateRedditCommentsUptoNgramCounts(year, month, order int) error {
	cacheExists := true
	for i := 0; i < order; i++ {
		datapath, err := GetRedditNgramsLocalPath(year, month, i+1)
		if err != nil {
			return err
		} else if _, err := os.Stat(datapath); os.IsNotExist(err) {
			cacheExists = false
			break
		}
	}

	if cacheExists {
		log.Printf("Cache exists for all request n-grams. Skipping %04d-%02d\n", year, month)
		return nil
	}

	uptoNgramCounts, err := CountRedditCommentsUptoNgrams(year, month, order)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(order)
	for i, mgramCounts := range uptoNgramCounts {
		go func(counts map[string]uint64, order int) {
			defer wg.Done()
			WriteRedditNgramCounts(counts, year, month, order)
		}(mgramCounts, i+1)
	}

	wg.Wait()
	return nil
}

func CountRedditCommentsUptoNgrams(year, month, order int) ([]map[string]uint64, error) {
	var months []int
	if month == 0 {
		months = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	} else {
		months = []int{month}
	}

	uptoNgramCounts := make([]map[string]uint64, order)
	uptoNgramStreamsMerged := make([]chan []string, order)
	for i := 0; i < order; i++ {
		uptoNgramCounts[i] = make(map[string]uint64)
		uptoNgramStreamsMerged[i] = make(chan []string)
	}

	go func() {
		for i := 0; i < order; i++ {
			defer close(uptoNgramStreamsMerged[i])
		}
		var wg sync.WaitGroup
		for _, month := range months {
			uptoNgramStreams, err := ExtractRedditCommentsUptoNgramStreams(year, month, order)
			if err != nil {
				if isDateOutOfRangeEorr(err) {
					continue
				} else {
					log.Fatalln(err)
				}
			}
			for i := 0; i < order; i++ {
				wg.Add(1)
				go func(s chan []string, r chan []string) {
					defer wg.Done()
					for v := range s {
						r <- v
					}
				}(uptoNgramStreams[i], uptoNgramStreamsMerged[i])
			}
		}
		wg.Wait()
	}()

	var wg sync.WaitGroup
	wg.Add(order)
	for i := 0; i < order; i++ {
		go func(ngramStream chan []string, ngramCounts map[string]uint64) {
			defer wg.Done()
			for ngram := range ngramStream {
				ngramCounts[Tokens2String(ngram)] += 1
			}
		}(uptoNgramStreamsMerged[i], uptoNgramCounts[i])
	}

	wg.Wait()
	return uptoNgramCounts, nil
}

func ExtractRedditCommentsUptoNgramStreams(year, month, order int) ([]chan []string, error) {
	uptoNgramSamples, err := ExtractRedditCommentsUptoNgramSamples(year, month, order)
	if err != nil {
		return nil, err
	}

	uptoNgramStreams := make([]chan []string, order)
	for i := 0; i < order; i++ {
		uptoNgramStreams[i] = make(chan []string)
		go func(samples chan [][]string, stream chan []string) {
			defer close(stream)
			for ngrams := range samples {
				for _, ngram := range ngrams {
					stream <- ngram
				}
			}
		}(uptoNgramSamples[i], uptoNgramStreams[i])
	}

	return uptoNgramStreams, nil
}

func ExtractRedditCommentsUptoNgramSamples(year, month, order int) ([]chan [][]string, error) {
	comments, err := LoadRedditCommentsJson(year, month)
	if err != nil {
		return nil, err
	}

	uptoNgrams := make([]chan [][]string, order)
	for i := 0; i < order; i++ {
		uptoNgrams[i] = make(chan [][]string)
	}

	go func() {
		for _, ch := range uptoNgrams {
			defer close(ch)
		}
		for comment := range comments {
			tokens := String2Tokens(comment.Body)
			for i, mgramStrs := range ExtractFilteredUptoNgrams(tokens, order, MaxRedditTokenLength) {
				uptoNgrams[i] <- mgramStrs
			}
		}
	}()

	return uptoNgrams, nil
}

func ExtractFilteredUptoNgrams(tokens []string, order int, maxTokLen int) [][][]string {
	uptoNgrams := make([][][]string, order)
	for i := 0; i < order; i++ {
		uptoNgrams[i] = ExtractFilteredNgrams(tokens, i+1, maxTokLen)
	}
	return uptoNgrams
}

func ExtractFilteredNgrams(tokens []string, order int, maxTokLen int) [][]string {
	length := len(tokens) - order + 1
	if length < 1 {
		return [][]string{}
	}
	ngrams := make([][]string, length)
	for i := 0; i < length; i++ {
		ngram := tokens[i : i+order]
		if !HasLongToken(ngram, maxTokLen) {
			ngrams[i] = ngram
		}
	}
	return ngrams
}

func ExtractNgrams(tokens []string, order int) [][]string {
	length := len(tokens) - order + 1
	if length < 1 {
		return [][]string{}
	}

	ngrams := make([][]string, length)
	for i := 0; i < length; i++ {
		ngram := tokens[i : i+order]
		ngrams[i] = ngram
	}
	return ngrams
}

func HasLongToken(tokens []string, maxTokLen int) bool {
	for _, tok := range tokens {
		if len(tok) > maxTokLen {
			return true
		}
	}
	return false
}

func String2Tokens(text string) []string {
	return strings.Fields(text)
}

func Tokens2String(tokens []string) string {
	return strings.Join(tokens, " ")
}
