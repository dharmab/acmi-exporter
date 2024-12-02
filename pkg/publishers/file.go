package publishers

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

// FilePublisher publishes ACMI messages to a file.
type FilePublisher struct {
	// Folder where the file will be saved.
	Folder string
	// Title of the mission.
	Title string
}

var _ Publisher = &FilePublisher{}

// Publish implements [Publisher.Publish] by writing messages to a file. The file is created in the FilePublisher's folder, and is named using the FilePublisher's title and the current date and time.
func (p *FilePublisher) Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error {
	path := fmt.Sprintf("%s/%s %s.acmi", p.Folder, p.Title, time.Now().Format("2006-01-02-150405"))
	logger := log.With().Str("path", path).Logger()
	logger.Info().Msg("creating file")
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	logger.Info().Msg("writing file headers")
	if _, err := file.WriteString("FileType=text/acmi/tacview\nFileVersion=2.2\n"); err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	logger.Info().Msg("writing data to file")
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
