package publishers

import (
	"context"
	"fmt"
)

type StdoutPublisher struct{}

var _ Publisher = &StdoutPublisher{}

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
