package publishers

import (
	"context"
	"fmt"
	"os"
	"time"
)

type FilePublisher struct {
	Folder string
	Title  string
}

var _ Publisher = &FilePublisher{}

func (p *FilePublisher) Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error {
	path := fmt.Sprintf("%s/%s %s.acmi", p.Folder, p.Title, time.Now().Format("2006-01-02-150405"))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString("FileType=text/acmi/tacview\nFileVersion=2.2\n"); err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	initialLines, err := initials.Get()
	if err != nil {
		return fmt.Errorf("failed to get initials: %w", err)
	}
	for _, line := range initialLines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write initial line to file: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-messages:
			line := message + "\n"
			if _, err := file.WriteString(line); err != nil {
				return fmt.Errorf("failed to write message to file: %w", err)
			}
		}
	}
}
