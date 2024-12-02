package streamer

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/DCS-gRPC/go-bindings/dcs/v0/coalition"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/common"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/hook"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/mission"
	"github.com/dharmab/goacmi/objects"
	"github.com/dharmab/goacmi/properties"
	"github.com/dharmab/goacmi/properties/coalitions"
	"github.com/dharmab/goacmi/properties/colors"
	"github.com/dharmab/goacmi/tags"
	measure "github.com/martinlindhe/unit"
	"github.com/rs/zerolog/log"
)

type Payload struct {
	Update      *objects.Update
	MissionTime time.Duration
}

type Streamer struct {
	missionServiceClient   mission.MissionServiceClient
	coalitionServiceClient coalition.CoalitionServiceClient
	hookServiceClient      hook.HookServiceClient
}

func New(
	missionServiceClient mission.MissionServiceClient,
	coalitionServiceClient coalition.CoalitionServiceClient,
	hookServiceClient hook.HookServiceClient,
) *Streamer {
	return &Streamer{
		missionServiceClient:   missionServiceClient,
		coalitionServiceClient: coalitionServiceClient,
		hookServiceClient:      hookServiceClient,
	}
}

func (s *Streamer) Stream(ctx context.Context, updates chan<- Payload, airUpdateInterval, surfaceUpdateInterval, weaponUpdateInterval time.Duration) {
	var wg sync.WaitGroup
	streamCtx, cancel := context.WithCancel(ctx)

	wg.Add(5)
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamCategory(streamCtx, common.GroupCategory_GROUP_CATEGORY_AIRPLANE, updates, airUpdateInterval)
		log.Info().Msg("streaming complete")
	}()
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamCategory(streamCtx, common.GroupCategory_GROUP_CATEGORY_HELICOPTER, updates, airUpdateInterval)
	}()
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamCategory(streamCtx, common.GroupCategory_GROUP_CATEGORY_GROUND, updates, surfaceUpdateInterval)
	}()
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamCategory(streamCtx, common.GroupCategory_GROUP_CATEGORY_SHIP, updates, surfaceUpdateInterval)
	}()
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamCategory(streamCtx, common.GroupCategory_GROUP_CATEGORY_UNSPECIFIED, updates, surfaceUpdateInterval)
	}()
}

func (s *Streamer) GetGlobalObject(ctx context.Context) (*objects.Object, error) {
	global := objects.New(0)

	title, err := s.GetMissionName(ctx)
	if err != nil {
		return nil, err
	}
	global.SetProperty(properties.Title, title)

	refTime, err := s.GetStartTime(ctx)
	if err != nil {
		return nil, err
	}
	global.SetProperty(properties.ReferenceTime, refTime)
	recTime, err := s.GetCurrentTime(ctx)
	if err != nil {
		return nil, err
	}
	global.SetProperty(properties.RecordingTime, recTime)

	global.SetProperty(properties.DataRecorder, "acmi-exporter")
	global.SetProperty(properties.DataSource, "DCS World")
	global.SetProperty(properties.ReferenceLongitude, "0")
	global.SetProperty(properties.ReferenceLatitude, "0")

	return global, nil
}

func (s *Streamer) GetMissionName(ctx context.Context) (string, error) {
	resp, err := s.hookServiceClient.GetMissionName(ctx, &hook.GetMissionNameRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetName(), nil
}

func (s *Streamer) GetStartTime(ctx context.Context) (string, error) {
	resp, err := s.missionServiceClient.GetScenarioStartTime(ctx, &mission.GetScenarioStartTimeRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetDatetime(), nil
}

func (s *Streamer) GetCurrentTime(ctx context.Context) (string, error) {
	resp, err := s.missionServiceClient.GetScenarioCurrentTime(ctx, &mission.GetScenarioCurrentTimeRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetDatetime(), nil
}

const (
	blueBullseyeID    = 0x40000001
	neutralBullseyeID = 0x40000002
	redBullseyeID     = 0x40000003
)

func (s Streamer) GetBullseyes(ctx context.Context) ([]*objects.Object, error) {
	bullseyes := make([]*objects.Object, 0)
	for _, c := range []common.Coalition{common.Coalition_COALITION_BLUE, common.Coalition_COALITION_NEUTRAL, common.Coalition_COALITION_RED} {
		resp, err := s.coalitionServiceClient.GetBullseye(ctx, &coalition.GetBullseyeRequest{Coalition: c})
		if err != nil {
			return nil, err
		}
		bullseyes = append(bullseyes, s.buildBullseye(resp, c))
	}
	return bullseyes, nil
}

func (s *Streamer) buildBullseye(resp *coalition.GetBullseyeResponse, c common.Coalition) *objects.Object {
	if resp == nil {
		return nil
	}
	bullseye := &objects.Object{
		Properties: map[string]string{
			properties.Type:      strings.Join([]string{"Navaid", tags.Static, tags.Bullseye}, "+"),
			properties.Coalition: convertCoalition(c),
			properties.Color:     coalitionColor(c),
		},
	}
	if position := resp.GetPosition(); position != nil {
		altitude := measure.Length(position.GetAlt()) * measure.Meter
		coordinates := objects.NewCoordinates(
			&position.Lon, &position.Lat,
			&altitude,
			&position.U, &position.V,
			nil, nil, nil, nil,
		)
		bullseye.Properties[properties.Transform] = coordinates.Transform(0, 0)
	}
	switch c {
	case common.Coalition_COALITION_RED:
		bullseye.ID = redBullseyeID
	case common.Coalition_COALITION_BLUE:
		bullseye.ID = blueBullseyeID
	case common.Coalition_COALITION_NEUTRAL:
		bullseye.ID = neutralBullseyeID
	}
	return bullseye
}

func (s *Streamer) streamCategory(ctx context.Context, category common.GroupCategory, updates chan<- Payload, interval time.Duration) {
	pollRate := uint32(interval.Seconds())
	request := &mission.StreamUnitsRequest{PollRate: &pollRate, Category: category}
	log.Info().Str("category", category.String()).Msg("streaming units")
	stream, err := s.missionServiceClient.StreamUnits(ctx, request)
	if err != nil {
		log.Error().Err(err).Msg("failed to stream units")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				continue
			}
			if err != nil {
				log.Error().Err(err).Msg("received error from units stream")
				return
			}
			payload := Payload{
				Update:      s.buildUpdate(response),
				MissionTime: time.Second * time.Duration(response.GetTime()),
			}
			log.Debug().Str("update", payload.Update.String()).Msg("prepared update")
			updates <- payload
		}
	}
}

func (s *Streamer) buildUpdate(resp *mission.StreamUnitsResponse) *objects.Update {
	var update *objects.Update
	if gone := resp.GetGone(); gone != nil {
		update = &objects.Update{
			ID:        uint64(gone.GetId()),
			IsRemoval: true,
		}
	} else if _unit := resp.GetUnit(); _unit != nil {
		update = &objects.Update{
			ID:        uint64(_unit.GetId()),
			IsRemoval: false,
			Properties: map[string]string{
				properties.Type:      s.buildType(_unit),
				properties.Transform: s.buildCoordinates(_unit).Transform(0, 0),
			},
		}

		if _unit.Type != "" {
			update.Properties[properties.Name] = _unit.Type
		}
		if _unit.PlayerName != nil && *_unit.PlayerName != "" {
			update.Properties[properties.Pilot] = *_unit.PlayerName
		} else if _unit.Name != "" {
			update.Properties[properties.Pilot] = _unit.Name
		}
		if _unit.Group != nil && _unit.Group.Name != "" {
			update.Properties[properties.Group] = _unit.Group.Name
		}
		update.Properties[properties.Coalition] = convertCoalition(_unit.GetCoalition())
		update.Properties[properties.Color] = coalitionColor(_unit.GetCoalition())
	}
	return update
}

func convertCoalition(c common.Coalition) string {
	switch c {
	case common.Coalition_COALITION_RED:
		return coalitions.Allies.String()
	case common.Coalition_COALITION_BLUE:
		return coalitions.Enemies.String()
	case common.Coalition_COALITION_NEUTRAL:
		return coalitions.Neutrals.String()
	}
	return ""
}

func coalitionColor(c common.Coalition) string {
	switch c {
	case common.Coalition_COALITION_RED:
		return colors.Red.String()
	case common.Coalition_COALITION_BLUE:
		return colors.Blue.String()
	case common.Coalition_COALITION_NEUTRAL:
		return colors.Grey.String()
	}
	return ""
}

func (s *Streamer) buildType(_unit *common.Unit) string {
	types := []string{}
	if group := _unit.GetGroup(); group != nil {
		switch group.GetCategory() {
		case common.GroupCategory_GROUP_CATEGORY_AIRPLANE:
			types = append(types, tags.Air, tags.FixedWing)
		case common.GroupCategory_GROUP_CATEGORY_HELICOPTER:
			types = append(types, tags.Air, tags.Rotorcraft)
		case common.GroupCategory_GROUP_CATEGORY_TRAIN:
			types = append(types, tags.Ground)
		case common.GroupCategory_GROUP_CATEGORY_GROUND:
			types = append(types, tags.Ground)
		case common.GroupCategory_GROUP_CATEGORY_SHIP:
			types = append(types, tags.Sea)
		}
	}
	return strings.Join(types, "+")
}

func (s *Streamer) buildCoordinates(_unit *common.Unit) *objects.Coordinates {
	var lon, lat *float64
	var altitude *measure.Length
	var u, v *float64
	if position := _unit.GetPosition(); position != nil {
		lon = &position.Lon
		lat = &position.Lat

		a := measure.Length(position.GetAlt()) * measure.Meter
		altitude = &a

		u = &position.U
		v = &position.V
	}

	var roll, pitch, yaw, heading *measure.Angle
	if orientation := _unit.GetOrientation(); orientation != nil {
		r := measure.Angle(orientation.GetRoll()) * measure.Degree
		roll = &r

		p := measure.Angle(orientation.GetPitch()) * measure.Degree
		pitch = &p

		y := measure.Angle(orientation.GetYaw()) * measure.Degree
		yaw = &y

		h := measure.Angle(orientation.GetHeading()) * measure.Degree
		heading = &h
	}

	return objects.NewCoordinates(lon, lat, altitude, u, v, roll, pitch, yaw, heading)
}
