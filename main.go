package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const (
	chunkSize           = 10 * 1024 * 1024 // 10 MB
	maxRetries          = 3
	retryDelay          = 2 * time.Second
	maxConcurrentChunks = 8
)

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

func downloadAll(ctx context.Context, client *http.Client, p *mpb.Progress, urls []string, savePath string) []error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(urls))

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			err := downloadFile(ctx, client, p, u, savePath)
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

func downloadFile(ctx context.Context, client *http.Client, p *mpb.Progress, url, savePath string) error {

	info, err := fetchFileInfo(client, url)
	if err != nil {
		return err
	}
	if info.size < 0 {
		return fmt.Errorf("размер файла неизвестен")
	}

	filename := path.Base(url)
	fullPath := filepath.Join(savePath, filename)
	statePath := fullPath + ".progress"

	n := countChunks(info.size)

	state, found, err := loadState(statePath)
	if err != nil {
		return err
	}

	if found && (state.URL != url || state.TotalSize != info.size || state.ChunkSize != chunkSize || len(state.DownloadedChunks) != int(n)) {
		found = false
	}

	if !found {
		state = DownloadState{
			URL:              url,
			TotalSize:        info.size,
			ChunkSize:        chunkSize,
			TotalChunks:      int(n),
			DownloadedChunks: make([]bool, n),
		}

		if err := saveState(statePath, state); err != nil {
			return err
		}
	}

	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(info.size); err != nil {
		return err
	}

	bar := p.AddBar(info.size,
		mpb.PrependDecorators(decor.Name(filename, decor.WCSyncWidthR)),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.Counters(decor.SizeB1024(0), "% .1f / % .1f", decor.WCSyncSpace),
		),
	)
	defer bar.Abort(false)

	var done int64
	for i := range state.DownloadedChunks {
		if state.DownloadedChunks[i] {
			start, end := chunkBounds(int64(i), info.size)
			done += end - start + 1
		}
	}
	bar.SetCurrent(done)

	jobs := make(chan int64, n)
	var wg sync.WaitGroup
	var stateMu sync.Mutex
	var fileMu sync.Mutex

	for i := range n {
		if state.DownloadedChunks[i] {
			continue
		}
		jobs <- i
	}
	close(jobs)

	for w := 0; w < maxConcurrentChunks; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case i, ok := <-jobs:
					if !ok {
						return
					}
					start, end := chunkBounds(i, info.size)
					if err := downloadChunkWithRetry(client, p, url, file, &fileMu, i, n, start, end); err != nil {
						fmt.Fprintf(p, "%s: %v, переходим к следующему чанку\n", filename, err)
						continue
					}

					stateMu.Lock()
					state.DownloadedChunks[i] = true
					if err := saveState(statePath, state); err != nil {
						fmt.Fprintf(p, "%s: ошибка сохранения состояния: %v\n", filename, err)
					}
					stateMu.Unlock()

					bar.IncrBy(int(end - start + 1))
				}
			}
		}()
	}
	wg.Wait()

	if ctx.Err() != nil {
		if err := saveState(statePath, state); err != nil {
			return err
		}
		fmt.Fprintf(p, "%s: состояние сохранено\n", filename)
		return ctx.Err()
	}

	return nil
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

func countChunks(fileSize int64) int64 {
	return (fileSize + chunkSize - 1) / chunkSize
}

func chunkBounds(i, fileSize int64) (start, end int64) {
	start = i * chunkSize
	end = min((i+1)*chunkSize-1, fileSize-1)
	return start, end
}

func downloadChunkWithRetry(client *http.Client, p *mpb.Progress, url string, file *os.File, fileMu *sync.Mutex, i, n, start, end int64) error {
	var err error
	for attempt := range maxRetries {
		err = downloadChunk(client, url, file, fileMu, i, n, start, end)
		if err == nil {
			break
		}

		fmt.Fprintf(p, "Загрузка чанка %d/%d... ошибка: %v\n", i+1, n, err)
		if attempt < maxRetries-1 {
			fmt.Fprintf(p, "  Повторная попытка через %v (%d/%d)...\n", retryDelay, attempt+1, maxRetries)
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("чанк %d/%d: не удалось загрузить после %d попыток: %w", i+1, n, maxRetries, err)
	}

	return nil
}

func downloadChunk(client *http.Client, url string, file *os.File, fileMu *sync.Mutex, i, n, start, end int64) error {
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

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fileMu.Lock()
	_, err = file.WriteAt(data, start)
	fileMu.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func loadState(statePath string) (DownloadState, bool, error) {
	_, err := os.Stat(statePath)
	if os.IsNotExist(err) {
		return DownloadState{}, false, nil
	}
	if err != nil {
		return DownloadState{}, false, err
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		return DownloadState{}, false, err
	}

	var state DownloadState

	if err := json.Unmarshal(data, &state); err != nil {
		return DownloadState{}, false, err
	}

	return state, true, nil
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := mpb.New()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		fmt.Fprintln(p, "\nПолучен сигнал прерывания, завершаем работу...")
		fmt.Fprintln(p, "Ожидание завершения текущих загрузок...")
		cancel()
	}()

	client := &http.Client{Timeout: 30 * time.Second}

	errs := downloadAll(ctx, client, p, urls, savePath)
	p.Wait()
	if ctx.Err() != nil {
		fmt.Println("Выход")
		os.Exit(1)
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Println("Ошибка:", e)
		}
		os.Exit(1)
	}

	fmt.Println("Все файлы загружены!")
}
