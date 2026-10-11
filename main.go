package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const chunkSize = 10 * 1024 * 1024 // 10 MB

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
	out := formatInfo(filename, info)
	if info.size >= 0 {
		out += formatChunks(info.size)
	}
	fmt.Print(out)

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

	if info.size >= 0 {
		if err := file.Truncate(info.size); err != nil {
			return err
		}
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("Файл сохранён:", fullPath)

	return nil
}

func downloadAll(client *http.Client, urls []string, savePath string) []error {
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

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	return errs
}

func countChunks(fileSize int64) int64 {
	return (fileSize + chunkSize - 1) / chunkSize
}

func chunkBounds(i, fileSize int64) (start, end int64) {
	start = i * chunkSize
	end = min((i+1)*chunkSize-1, fileSize-1)
	return start, end
}

func formatChunks(fileSize int64) string {
	n := countChunks(fileSize)

	var b strings.Builder
	fmt.Fprintf(&b, "Размер чанка: %d байт\n", chunkSize)
	fmt.Fprintf(&b, "Количество чанков: %d\n", n)

	for i := range n {
		start, end := chunkBounds(i, fileSize)
		fmt.Fprintf(&b, "Чанк %d/%d: байты %d-%d\n", i+1, n, start, end)
	}

	return b.String()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	err := os.MkdirAll(savePath, 0755)
	if err != nil {
		fmt.Println("Не удалось создать директорию:", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: 30 * time.Second}

	if errs := downloadAll(client, urls, savePath); len(errs) > 0 {
		for _, e := range errs {
			fmt.Println("Ошибка:", e)
		}
		os.Exit(1)
	}
	fmt.Println("Все файлы загружены!")
}
