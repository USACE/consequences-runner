package hazardproviders

import (
	"github.com/HydrologicEngineeringCenter/go-statistics/statistics"
	"github.com/USACE/go-consequences/geography"
	gc "github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
)

type SingleParameter_Mean_and_stdev_HazardProvider struct {
	meandepthcr   cogReader
	stevdepthcr   cogReader
	VerticalSlice []float64
	Process       gc.HazardFunction
}

func InitSingleParameter(mdfp string, sdfp string, verticalSlice []float64) (SingleParameter_Mean_and_stdev_HazardProvider, error) {
	md, err := initCR(mdfp)
	if err != nil {
		return SingleParameter_Mean_and_stdev_HazardProvider{}, err
	}
	sd, err := initCR(sdfp)
	if err != nil {
		return SingleParameter_Mean_and_stdev_HazardProvider{}, err
	}
	return SingleParameter_Mean_and_stdev_HazardProvider{meandepthcr: md, stevdepthcr: sd, VerticalSlice: verticalSlice}, err
}
func (hp SingleParameter_Mean_and_stdev_HazardProvider) Close() {
	hp.meandepthcr.Close()
	hp.stevdepthcr.Close()
}
func (hp *SingleParameter_Mean_and_stdev_HazardProvider) SetProcess(function gc.HazardFunction) {
	hp.Process = function
}
func (chp SingleParameter_Mean_and_stdev_HazardProvider) Hazards(l geography.Location) ([]hazards.HazardEvent, error) {
	var h []hazards.HazardEvent
	md, err := chp.meandepthcr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	sd, err := chp.stevdepthcr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	depthDist := statistics.NormalDistribution{
		Mean:              md,
		StandardDeviation: sd,
	}
	for _, p := range chp.VerticalSlice {
		d := depthDist.InvCDF(p)
		hd := hazards.HazardData{
			Depth: d,
		}
		estimate, err := chp.Process(hd, nil)
		if err != nil {
			return h, err
		}
		h = append(h, estimate)
	}
	return h, err
}

func (chp SingleParameter_Mean_and_stdev_HazardProvider) HazardBoundary() (geography.BBox, error) {
	return chp.meandepthcr.GetBoundingBox()
}
func (chp SingleParameter_Mean_and_stdev_HazardProvider) SpatialReference() string {
	return chp.meandepthcr.SpatialReference()
}
func (chp SingleParameter_Mean_and_stdev_HazardProvider) UpdateSpatialReference(sr_wkt string) {
	chp.meandepthcr.UpdateSpatialReference(sr_wkt)
}
