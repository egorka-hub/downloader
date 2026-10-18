package main

import (
	"encoding/json"
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

const chunkSize = 10 * 1024 * 1024 // 10 MB

type fileInfo struct {
	size           int64
	supportsResume bool
}

type DownloadState struct {
	URL              string `json:"url"`
	TotalSize        int64  `json:"total_size"`
	ChunkSize        int    `json:"chunk_size"`
	TotalChunks      int    `json:"total_chunks"`
	DownloadedChunks []bool `json:"downloaded_chunks"`
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
		return err
	}
	if info.size < 0 {
		return fmt.Errorf("размер файла неизвестен")
	}

	filename := path.Base(url)
	fmt.Print(formatInfo(filename, info))

	fullPath := filepath.Join(savePath, filename)
	statePath := fullPath + ".progress"

	n := countChunks(info.size)

	state := DownloadState{
		URL:              url,
		TotalSize:        info.size,
		ChunkSize:        chunkSize,
		TotalChunks:      int(n),
		DownloadedChunks: make([]bool, n),
	}

	if err := saveState(statePath, state); err != nil {
		return err
	}

	fmt.Println("Создан файл состояния:", filepath.Base(statePath))

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(info.size); err != nil {
		return err
	}

	for i := range n {
		start, end := chunkBounds(i, info.size)
		if err := downloadChunk(client, url, file, i, n, start, end); err != nil {
			return err
		}
		state.DownloadedChunks[i] = true
		if err := saveState(statePath, state); err != nil {
			return err
		}
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

func downloadChunk(client *http.Client, url string, file *os.File, i, n, start, end int64) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("чанк %d/%d: сервер вернул %d", i+1, n, resp.StatusCode)
	}

	_, err = file.Seek(start, io.SeekStart)
	if err != nil {
		return fmt.Errorf("ошибка при установке позиции в файле: %w", err)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("Загрузка чанка %d/%d → записан в позицию %d\n", i+1, n, start)

	return nil
}

func saveState(statePath string, state DownloadState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(statePath, data, 0644)
	if err != nil {
		return err
	}

	return nil
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
