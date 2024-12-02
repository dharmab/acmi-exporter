package publishers

import (
	"context"
	"fmt"

	"github.com/dharmab/goacmi/objects"
	"github.com/dharmab/goacmi/properties"
)

type Publisher interface {
	Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error
}

type InitialsProvider interface {
	Get() ([]string, error)
}

type Initials struct {
	Global    *objects.Object
	Bullseyes []*objects.Object
}

func (i *Initials) Get() ([]string, error) {
	lines := []string{}
	for _, propName := range []string{
		properties.ReferenceTime,
		properties.RecordingTime,
		properties.Title,
		properties.DataRecorder,
		properties.DataSource,
		properties.ReferenceLongitude,
		properties.ReferenceLatitude,
	} {
		value, ok := i.Global.GetProperty(propName)
		if !ok {
			return lines, fmt.Errorf("missing global property %q", propName)
		}
		update := objects.Update{
			ID:         i.Global.ID,
			IsRemoval:  false,
			Properties: map[string]string{propName: value},
		}
		lines = append(lines, update.String())
	}

	for _, obj := range i.Bullseyes {
		lines = append(lines, obj.String())
	}

	return lines, nil
}
