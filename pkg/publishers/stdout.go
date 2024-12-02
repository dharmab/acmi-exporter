package publishers

import (
	"context"
	"fmt"
)

// StdoutPublisher publishes ACMI messages to stdout.
type StdoutPublisher struct{}

var _ Publisher = &StdoutPublisher{}

// Publish implements [Publisher.Publish] by writing messages to stdout.
func (p *StdoutPublisher) Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error {
	i, err := initials.Get()
	if err != nil {
		return err
	}
	for s := range i {
		fmt.Println(s)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-messages:
			fmt.Println(message)
		}
	}
}
