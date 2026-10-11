package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	fmt.Println("Директория для сохранения:", savePath)
	fmt.Println("URL для скачивания:")
	for _, url := range urls {
		fmt.Println("  -", url)
	}
}
