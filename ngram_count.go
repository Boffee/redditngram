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
		go func(counts *StringCounter, order int) {
			defer wg.Done()
			counts.RLock()
			WriteRedditNgramCounts(counts.GetMap(), year, month, order)
			counts.RUnlock()
		}(mgramCounts, i+1)
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

// func GenerateRedditCommentsUptoNgramCountsHashed(year, month, order int) error {
// 	cacheExists, err := redditUptoNgramCacheExists(year, month, order)
// 	if err != nil {
// 		return err
// 	}
// 	if cacheExists {
// 		log.Printf("Cache exists for all requested n-grams. Skipping %04d-%02d\n", year, month)
// 		return nil
// 	}

// 	uptoNgramCounts, uniqueUptoNgramStrStreams, err := CountRedditCommentsUptoNgramsHashed(year, month, order)
// 	if err != nil {
// 		return err
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(order)
// 	for i, uniqueMgramStrStreams := range uniqueUptoNgramStrStreams {
// 		go func() {

// 		}()
// 	}

// 	wg.Add(order)
// 	for i, mgramCounts := range uptoNgramCounts {
// 		go func(counts *StringCounter, order int) {
// 			defer wg.Done()
// 			counts.RLock()
// 			WriteRedditNgramCounts(counts.GetMap(), year, month, order)
// 			counts.RUnlock()
// 		}(mgramCounts, i+1)
// 	}

// 	wg.Wait()
// 	return nil
// }

func CountRedditCommentsUptoNgramsHashed(year, month, order int) ([]*HashCounter, []chan string, error) {
	uptoNgramCounts := make([]*HashCounter, order)
	uniqueUptoNgramStrStreams := make([]chan string, order)
	for i := 0; i < order; i++ {
		uptoNgramCounts[i] = NewHashCounter()
		uniqueUptoNgramStrStreams[i] = make(chan string)
	}

	go func() {
		for _, uniqueMgramStreams := range uniqueUptoNgramStrStreams {
			defer close(uniqueMgramStreams)
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
					ngramCounts *HashCounter,
					uniqueNgramStrStreams chan string) {
					defer wg.Done()
					var ngramStr string
					for ngram := range ngramStream {
						ngramStr = Tokens2String(ngram)
						ngramCounts.Add([]byte(ngramStr))
						uniqueNgramStrStreams <- ngramStr
					}
				}(uptoNgramStreams[i], uptoNgramCounts[i], uniqueUptoNgramStrStreams[i])
			}
		}
		wg.Wait()
	}()

	return uptoNgramCounts, uniqueUptoNgramStrStreams, nil
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
