package actions

import (
	"fmt"
	"testing"

	"github.com/usace/cc-go-sdk"
)

func Test_ComputeSingleEvent(t *testing.T) {
	a := cc.Action{
		Name:        "compute-event",
		Type:        "compute-event",
		Description: "compute-event",
		Parameters: map[string]any{
			"tableName":       "BluestoneLocal",
			"bucket":          "kanawha-pilot",
			"Inventory":       "/app/data/BluestoneLocal_unadjusted.gpkg",
			"inventoryDriver": "GPKG",
			"outputDriver":    "ESRI Shapefile",
			"outputFileName":  "/app/data/BluestoneLocal_unadjusted_consequences.shp",
		},
	}
	ComputeEvent(a)
}

func Test_ComputeMultiFrequency(t *testing.T) {
	a := cc.Action{
		Name:        "compute-frequency",
		Type:        "compute-frequency",
		Description: "compute-frequency",
		Parameters: map[string]any{
			"tableName":        "Bluestone Local",
			"Inventory":        "/workspaces/consequences-runner/data/Bluestone Local_reprojected.gpkg",
			"inventoryDriver":  "GPKG",
			"frequencies":      ".1, .04, .02, .01, .005, .002",
			"depth-grids":      "/vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_10yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_25yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_50yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_100yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_200yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_500yr_realz_1.tif",
			"velocity-grids":   "/vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_10yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_25yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_50yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_100yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_200yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-kanawha/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024/aep-grids/BluestoneLocal/aep_depth_500yr_realz_1.tif",
			"outputDriver":     "GPKG",
			"outputFileName":   "/workspaces/consequences-runner/data/BluestoneLocal_consequences.gpkg",
			"damage-functions": "/workspaces/consequences-runner/data/Inland_FFRD_damageFunctions.json",
		},
	}
	err := ComputeFrequencyEvent(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
