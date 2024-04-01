package actions

import (
	"fmt"
	"testing"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
)

func Test_ComputePayload(t *testing.T) {
	datasetName := "Coal"
	path := "/vsis3/kanawha-pilot/FFRD_Kanawha_Compute/sims/ressim/1/grids/Coal_ModelTiff/depth.tif"
	hp, err := hazardproviders.Init_CustomFunction(path, func(valueIn hazards.HazardData, hazard hazards.HazardEvent) (hazards.HazardEvent, error) {
		if valueIn.Depth == 0 {
			return hazard, hazardproviders.NoHazardFoundError{}
		}
		process := hazardproviders.DepthHazardFunction()
		return process(valueIn, hazard)
	})
	structurepath := fmt.Sprintf("/vsis3/kanawha-pilot/FFRD_Kanawha_Compute/consequences/%v.parquet", datasetName)
	sp, err := structureprovider.InitStructureProvider(structurepath, datasetName, "PARQUET")
	if err != nil {
		t.Fail()
	}
	outfp := fmt.Sprintf("/workspaces/consequences-runner/data/%v_results_test2.parquet", datasetName)
	var rw consequences.ResultsWriter
	sr := hp.SpatialReference()
	rw, err = resultswriters.InitSpatialResultsWriter_WKT_Projected(outfp, datasetName, "PARQUET", sr)
	if err != nil {
		t.Fail()
	}
	Compute(hp, sp, rw)
}
