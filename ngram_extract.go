package redditngram

import (
	"strings"
)

const MaxRedditTokenLength = 25

func ExtractRedditCommentsUptoNgramStreams(year, month, order int) ([]chan []string, error) {
	uptoNgramSamples, err := ExtractRedditCommentsUptoNgramSamples(year, month, order)
	if err != nil {
		return nil, err
	}

	uptoNgramStreams := make([]chan []string, order)
	for i := 0; i < order; i++ {
		uptoNgramStreams[i] = make(chan []string, 1024)
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
	comments, err := LoadRedditComments(year, month)
	if err != nil {
		return nil, err
	}

	uptoNgrams := make([]chan [][]string, order)
	for i := 0; i < order; i++ {
		uptoNgrams[i] = make(chan [][]string, 1024)
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
