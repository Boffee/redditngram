package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/boffee/redditngram"
)

func main() {
	numWorkers := flag.Int("numworkers", 1, "Number of months to process in parallel.")
	maxOrder := flag.Int("maxorder", 5, "Max order of n-grams.")
	flag.Parse()

	dataStartDate := redditngram.DataStartDate
	dataEndDate := time.Now().AddDate(0, -1, 0)
	dataCurrDate := dataStartDate

	workerQ := make(chan bool, *numWorkers)
	defer close(workerQ)
	for i := 0; i < *numWorkers; i++ {
		workerQ <- true
	}

	var wg sync.WaitGroup
	for {
		if dataCurrDate.After(dataEndDate) {
			break
		}
		<-workerQ
		wg.Add(1)
		go func(year, month, maxOrder int) {
			err := redditngram.GenerateRedditCommentsUptoNgramCountsHashed(year, month, maxOrder)
			if err != nil {
				log.Println(err)
			}
			workerQ <- true
			wg.Done()
			log.Printf("Finished generating n-gram for %04d-%02d\n", year, month)
		}(dataCurrDate.Year(), int(dataCurrDate.Month()), *maxOrder)
		dataCurrDate = dataCurrDate.AddDate(0, 1, 0)
	}

	wg.Wait()
}
