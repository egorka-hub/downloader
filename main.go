package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type fileInfo struct {
	size           int64
	supportsResume bool
}

func formatInfo(name string, info fileInfo) string {
	size := "неизвестен"
	if info.size >= 0 {
		size = fmt.Sprintf("%d байт", info.size)
	}

	resume := "не поддерживается"
	start := "Начало загрузки целиком..."
	if info.supportsResume {
		resume = "поддерживается"
		start = "Начало загрузки..."
	}

	return fmt.Sprintf("Файл: %s\n  Размер: %s\n  Докачка: %s\n%s\n", name, size, resume, start)
}

func fetchFileInfo(client *http.Client, url string) (fileInfo, error) {
	info := fileInfo{size: -1}

	resp, err := client.Head(url)
	if err != nil {
		return info, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	contentLength := resp.Header.Get("Content-Length")
	if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
		info.size = size
	}

	acceptRanges := resp.Header.Get("Accept-Ranges")
	info.supportsResume = acceptRanges == "bytes"

	return info, nil
}

func downloadFile(client *http.Client, url, savePath string) error {
	info, err := fetchFileInfo(client, url)
	if err != nil {
		info = fileInfo{size: -1}
	}

	filename := path.Base(url)
	fmt.Print(formatInfo(filename, info))

	err = os.MkdirAll(savePath, 0755)
	if err != nil {
		return err
	}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	fullPath := filepath.Join(savePath, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("Файл сохранён:", fullPath)

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	client := &http.Client{Timeout: 30 * time.Second}

	var wg sync.WaitGroup
	errCh := make(chan error, len(urls))

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			err := downloadFile(client, u, savePath)
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", path.Base(u), err)
			}
		}(url)
	}
	wg.Wait()
	close(errCh)

	hasErr := false
	for err := range errCh {
		hasErr = true
		fmt.Println("Ошибка:", err)
	}

	if !hasErr {
		fmt.Println("Все файлы загружены!")
	} else {
		os.Exit(1)
	}
}
