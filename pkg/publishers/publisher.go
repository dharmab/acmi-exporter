package publishers

import (
	"context"
	"fmt"

	"github.com/dharmab/goacmi/objects"
	"github.com/dharmab/goacmi/properties"
)

// Publisher publishes ACMI messages to an output.
type Publisher interface {
	// Publish is a blocking function that publishes ACMI messages to an output until the context is cancelled.
	Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error
}

// InitialsProvider provides initial object state.
type InitialsProvider interface {
	// Get returns the ACMI lines for the initial global object and navaids state.
	Get() ([]string, error)
}

// Initials implements [InitialsProvider].
type Initials struct {
	Global    *objects.Object
	Bullseyes []*objects.Object
}

// Get implements [InitialsProvider.Get].
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
