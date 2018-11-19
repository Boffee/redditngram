package redditngram

import (
	"log"
	"sync"
)

func GenerateRedditCommentsUptoNgramCounts(year, month, order int) error {
	cacheExists, err := redditUptoNgramCountsCacheExists(year, month, order)
	if err != nil {
		return err
	}
	if cacheExists {
		log.Printf("Cache exists for all requested n-grams. Skipping %04d-%02d\n", year, month)
		return nil
	}

	uptoNgramCounts, err := CountRedditCommentsUptoNgrams(year, month, order)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(order)
	for i, mgramCounts := range uptoNgramCounts {
		go func(ngramCounts *StringCounter, order int) {
			defer wg.Done()
			ngramCounts.RLock()
			err := WriteRedditNgramCounts(ngramCounts.GetMap(), year, month, order)
			if err != nil {
				log.Panic(err)
			}
			ngramCounts.RUnlock()
		}(mgramCounts, i+1)
	}

	wg.Wait()
	return nil
}

func GenerateRedditCommentsUptoNgramCountsHashed(year, month, order int) error {
	cacheExists, err := redditUptoNgramCountsCacheExists(year, month, order)
	if err != nil {
		return err
	}
	if cacheExists {
		log.Printf("Cache exists for all requested n-grams. Skipping %04d-%02d\n", year, month)
		return nil
	}

	uptoNgramHashCounts, uptoNgramVocabs, err := CountRedditCommentsUptoNgramsHashed(year, month, order)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(order)
	for i := 0; i < order; i++ {
		go func(ngramHCounts *HashCounter, ngramVocab <-chan string, order int) {
			defer wg.Done()
			err := WriteRedditNgramCountsHashed(ngramHCounts, ngramVocab, year, month, order)
			if err != nil {
				log.Panic(err)
			}
		}(uptoNgramHashCounts[i], uptoNgramVocabs[i], i+1)
	}
	wg.Wait()

	return nil
}

func CountRedditCommentsUptoNgrams(year, month, order int) ([]*StringCounter, error) {
	uptoNgramCounts := make([]*StringCounter, order)
	for i := 0; i < order; i++ {
		uptoNgramCounts[i] = NewStringCounter()
	}

	var wg sync.WaitGroup
	for _, month := range getQueryMonths(month) {
		uptoNgramStreams, err := ExtractRedditCommentsUptoNgramStreams(year, month, order)
		if err != nil {
			if isDateOutOfRangeError(err) {
				continue
			} else {
				log.Fatalln(err)
			}
		}

		for i := 0; i < order; i++ {
			wg.Add(1)
			go func(ngramStream chan []string, ngramCounts *StringCounter) {
				defer wg.Done()
				for ngram := range ngramStream {
					ngramCounts.Add(Tokens2String(ngram))
				}
			}(uptoNgramStreams[i], uptoNgramCounts[i])
		}
	}

	wg.Wait()
	return uptoNgramCounts, nil
}

func CountRedditCommentsUptoNgramsHashed(year, month, order int) ([]*HashCounter, []chan string, error) {
	uptoNgramHashCounts := make([]*HashCounter, order)
	uptoNgramVocabs := make([]chan string, order)
	for i := 0; i < order; i++ {
		uptoNgramHashCounts[i] = NewHashCounter()
		uptoNgramVocabs[i] = make(chan string)
	}

	go func() {
		for _, mgramVocab := range uptoNgramVocabs {
			defer close(mgramVocab)
		}

		var wg sync.WaitGroup
		for _, month := range getQueryMonths(month) {
			uptoNgramStreams, err := ExtractRedditCommentsUptoNgramStreams(year, month, order)
			if err != nil {
				if isDateOutOfRangeError(err) {
					continue
				} else {
					log.Fatalln(err)
				}
			}

			for i := 0; i < order; i++ {
				wg.Add(1)
				go func(
					ngramStream chan []string,
					ngramHashCounts *HashCounter,
					ngramVocab chan string) {
					defer wg.Done()
					var ngramStr string
					for ngram := range ngramStream {
						ngramStr = Tokens2String(ngram)
						ngramHashCounts.Add([]byte(ngramStr))
						ngramVocab <- ngramStr
					}
				}(uptoNgramStreams[i], uptoNgramHashCounts[i], uptoNgramVocabs[i])
			}
		}
		wg.Wait()
	}()

	return uptoNgramHashCounts, uptoNgramVocabs, nil
}

// If month is 0, get all months
func getQueryMonths(month int) (months []int) {
	if month < 0 || month > 12 {
		log.Fatalf("Invalid month: %d", month)
	}
	if month == 0 {
		months = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	} else {
		months = []int{month}
	}
	return months
}
