package hazardproviders

import (
	"time"

	"github.com/HydrologicEngineeringCenter/go-statistics/statistics"
	"github.com/USACE/go-consequences/geography"
	gc "github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
)

type Mean_and_stdev_HazardProvider struct {
	meandepthcr     cogReader
	stevdepthcr     cogReader
	meanvelocitycr  cogReader
	stdevvelocitycr cogReader
	VerticalSlice   []float64
	//meandurationcr cogReader
	//stdevdurationcr cogReader
	Process gc.HazardFunction
}

func Init(mdfp string, sdfp string, mvfp string, svfp string, verticalSlice []float64) (Mean_and_stdev_HazardProvider, error) {
	md, err := initCR(mdfp)
	if err != nil {
		return Mean_and_stdev_HazardProvider{}, err
	}
	sd, err := initCR(sdfp)
	if err != nil {
		return Mean_and_stdev_HazardProvider{}, err
	}
	mv, err := initCR(mvfp)
	if err != nil {
		return Mean_and_stdev_HazardProvider{}, err
	}
	sv, err := initCR(svfp)
	if err != nil {
		return Mean_and_stdev_HazardProvider{}, err
	}
	return Mean_and_stdev_HazardProvider{meandepthcr: md, stevdepthcr: sd, meanvelocitycr: mv, stdevvelocitycr: sv, VerticalSlice: verticalSlice}, err
}
func (hp Mean_and_stdev_HazardProvider) Close() {
	hp.meandepthcr.Close()
	hp.stevdepthcr.Close()
	hp.meanvelocitycr.Close()
	hp.stdevvelocitycr.Close()
}
func (hp *Mean_and_stdev_HazardProvider) SetProcess(function gc.HazardFunction) {
	hp.Process = function
}
func (chp Mean_and_stdev_HazardProvider) Hazards(l geography.Location) ([]hazards.HazardEvent, error) {
	var h []hazards.HazardEvent
	md, err := chp.meandepthcr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	sd, err := chp.stevdepthcr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	mv, err := chp.meanvelocitycr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	sv, err := chp.stdevvelocitycr.ProvideValue(l)
	if err != nil {
		return h, err
	}
	depthDist := statistics.NormalDistribution{
		Mean:              md,
		StandardDeviation: sd,
	}
	velocityDist := statistics.NormalDistribution{
		Mean:              mv,
		StandardDeviation: sv,
	}
	for _, p := range chp.VerticalSlice {
		d := depthDist.InvCDF(p)
		v := velocityDist.InvCDF(p)
		hd := hazards.HazardData{
			Depth:       d,
			Velocity:    v,
			ArrivalTime: time.Time{},
			Erosion:     0,
			Duration:    0,
			WaveHeight:  0,
			Salinity:    false,
			Qualitative: "",
		}
		estimate, err := chp.Process(hd, nil)
		if err != nil {
			return h, err
		}
		h = append(h, estimate)
	}
	return h, err
}

func (chp Mean_and_stdev_HazardProvider) HazardBoundary() (geography.BBox, error) {
	return chp.meandepthcr.GetBoundingBox()
}
func (chp Mean_and_stdev_HazardProvider) SpatialReference() string {
	return chp.meandepthcr.SpatialReference()
}
func (chp Mean_and_stdev_HazardProvider) UpdateSpatialReference(sr_wkt string) {
	chp.meandepthcr.UpdateSpatialReference(sr_wkt)
}
