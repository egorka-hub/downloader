package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
)

func downloadFile(url, savePath string) error {
	err := os.MkdirAll(savePath, 0755)
	if err != nil {
		return err
	}

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	filename := path.Base(url)

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

	for _, url := range urls {
		fmt.Println("Скачивание:", url)
		err := downloadFile(url, savePath)
		if err != nil {
			fmt.Println("Ошибка:", err)
		}
	}
}
