package main

import (
	"log"
	"sync"
	"time"

	"github.com/boffee/redditngram"
)

const maxWorkers = 4
const maxOrder = 5

func main() {
	dataStartDate := redditngram.DataStartDate
	dataEndDate := time.Now().AddDate(0, -1, 0)
	dataCurrDate := dataStartDate

	workerQ := make(chan bool, maxWorkers)
	defer close(workerQ)
	for i := 0; i < maxWorkers; i++ {
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
		}(dataCurrDate.Year(), int(dataCurrDate.Month()), maxOrder)
		dataCurrDate = dataCurrDate.AddDate(0, 1, 0)
	}

	wg.Wait()
}
