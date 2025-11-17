package actions

/*
import (
	"fmt"
	"testing"

	"github.com/usace/cc-go-sdk"
)

func Test_ComputeEvent(t *testing.T) {
	a := cc.Action{
		Name:        "compute-event",
		Type:        "compute-event",
		Description: "compute-event",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":        "Duwamish_NSIv2022_Calibrated",
				"Inventory":        "/workspaces/consequences-runner/data/duwamish/Duwamish_NSIv2022_Calibrated.gpkg",
				"inventoryDriver":  "GPKG",
				"depth-grid":       "/vsis3/ffrd-computable/model-library/ffrd-duwamish/checkpoint-validation/simulations/validation/1/grids/duwamish-20241216/depth.tif",
				"velocity-grid":    "/vsis3/ffrd-computable/model-library/ffrd-duwamish/checkpoint-validation/simulations/validation/1/grids/duwamish-20241216/velocity.tif",
				"outputDriver":     "GPKG",
				"outputFileName":   "/workspaces/consequences-runner/data/duwamish/duwamish_consequences.gpkg",
				"damage-functions": "/workspaces/consequences-runner/data/Inland_FFRD_damageFunctions.json",
			}},
	}
	err := ComputeEvent(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

func Test_ComputeMultiFrequency(t *testing.T) {
	a := cc.Action{
		Name:        "compute-frequency",
		Type:        "compute-frequency",
		Description: "compute-frequency",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":        "Duwamish_NSIv2022_Calibrated",
				"Inventory":        "/workspaces/consequences-runner/data/duwamish/Duwamish_NSIv2022_Calibrated.gpkg",
				"inventoryDriver":  "GPKG",
				"frequencies":      ".1, .04, .02, .01, .005, .002",
				"depth-grids":      "/vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_10yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_25yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_50yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_100yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_200yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_depth_500yr_realz_1.tif",
				"velocity-grids":   "/vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_10yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_25yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_50yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_100yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_200yr_realz_1.tif, /vsis3/ffrd-computable/model-library/ffrd-duwamish/scenario-simple-levees/simulations/aep-grids/aep_velocity_500yr_realz_1.tif",
				"outputDriver":     "GPKG",
				"outputFileName":   "/workspaces/consequences-runner/data/scenario-simple-levees/simulations/summary-outputs/Duwamish_NSIv2022_Calibrated_frequency_based_output_withTriangle.gpkg",
				"damage-functions": "/workspaces/consequences-runner/data/Inland_FFRD_damageFunctions.json",
			}},
	}
	err := ComputeFrequencyEvent(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
func Test_ComputeFEMAMultiFrequency(t *testing.T) {
	consequencesName := "Upper New at Claytor_unadjusted.gpkg"
	hydraulicsName := "UpperNew"
	outputName := "UpperNew_unadjusted_consequences.shp"
	root := "/vsis3/ffrd-computable/model-library/ffrd-duwamish/checkpoint-validation/simulations/validation/aep-grids"
	a := cc.Action{
		Name:        "compute-fema-frequency",
		Type:        "compute-fema-frequency",
		Description: "compute-fema-frequency",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":            "nsi",
				"Inventory":            fmt.Sprintf("/workspaces/consequences-runner/data/%v/%v", hydraulicsName, consequencesName),
				"inventoryDriver":      "GPKG",
				"frequencies":          ".1, .04, .02, .01, .005, .002",
				"vertical-slice":       ".1, .2, .3, .4, .5, .6, .7, .8, .9",
				"mean-depth-grids":     fmt.Sprintf("%v/%v/aep_mean_depth_10yr.tif, %v/%v/aep_mean_depth_25yr.tif, %v/%v/aep_mean_depth_50yr.tif, %v/%v/aep_mean_depth_100yr.tif, %v/%v/aep_mean_depth_200yr.tif, %v/%v/aep_mean_depth_500yr.tif", root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName),
				"mean-velocity-grids":  fmt.Sprintf("%v/%v/aep_mean_velocity_10yr.tif, %v/%v/aep_mean_velocity_25yr.tif, %v/%v/aep_mean_velocity_50yr.tif, %v/%v/aep_mean_velocity_100yr.tif, %v/%v/aep_mean_velocity_200yr.tif, %v/%v/aep_mean_velocity_500yr.tif", root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName),
				"stdev-depth-grids":    fmt.Sprintf("%v/%v/aep_stdev_depth_10yr.tif, %v/%v/aep_stdev_depth_25yr.tif, %v/%v/aep_stdev_depth_50yr.tif, %v/%v/aep_stdev_depth_100yr.tif, %v/%v/aep_stdev_depth_200yr.tif, %v/%v/aep_stdev_depth_500yr.tif", root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName),
				"stdev-velocity-grids": fmt.Sprintf("%v/%v/aep_stdev_velocity_10yr.tif, %v/%v/aep_stdev_velocity_25yr.tif, %v/%v/aep_stdev_velocity_50yr.tif, %v/%v/aep_stdev_velocity_100yr.tif, %v/%v/aep_stdev_velocity_200yr.tif, %v/%v/aep_stdev_velocity_500yr.tif", root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName, root, hydraulicsName),
				"outputDriver":         "ESRI Shapefile",
				"outputFileName":       fmt.Sprintf("/workspaces/consequences-runner/data/results/%v/%v", hydraulicsName, outputName),
				"damage-functions":     "/workspaces/consequences-runner/data/Inland_FFRD_damageFunctions.json",
			}},
	}
	err := ComputeFEMAFrequencyEvent(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

func Test_ComputeFEMAMultiFrequency_SingleParameter(t *testing.T) {
	consequencesName := "Duwamish_NSIv2022_Calibrated.gpkg"
	hydraulicsName := "duwamish"
	outputName := "Duwamish_NSIv2022_Calibrated_consequences_production_single-parameter.gpkg"
	root := "/vsis3/ffrd-computable/model-library/ffrd-duwamish/production/simulations/aep-grids"
	a := cc.Action{
		Name:        "compute-fema-frequency",
		Type:        "compute-fema-frequency",
		Description: "compute-fema-frequency",
		IOManager: cc.IOManager{
			Attributes: map[string]any{
				"tableName":         "Duwamish_NSIv2022_Calibrated",
				"Inventory":         fmt.Sprintf("/workspaces/consequences-runner/data/%v/%v", hydraulicsName, consequencesName),
				"inventoryDriver":   "GPKG",
				"frequencies":       ".1, .05, .02, .01, .005, .002, .001, .0005",
				"vertical-slice":    ".1, .2, .3, .4, .5, .6, .7, .8, .9",
				"mean-depth-grids":  fmt.Sprintf("%v/aep_mean_depth_10yr.tif, %v/aep_mean_depth_20yr.tif, %v/aep_mean_depth_50yr.tif, %v/aep_mean_depth_100yr.tif, %v/aep_mean_depth_200yr.tif, %v/aep_mean_depth_500yr.tif, %v/aep_mean_depth_1000yr.tif, %v/aep_mean_depth_2000yr.tif", root, root, root, root, root, root, root, root),
				"stdev-depth-grids": fmt.Sprintf("%v/aep_stdev_depth_10yr.tif, %v/aep_stdev_depth_20yr.tif, %v/aep_stdev_depth_50yr.tif, %v/aep_stdev_depth_100yr.tif, %v/aep_stdev_depth_200yr.tif, %v/aep_stdev_depth_500yr.tif, %v/aep_stdev_depth_1000yr.tif, %v/aep_stdev_depth_2000yr.tif", root, root, root, root, root, root, root, root),
				"outputDriver":      "GPKG",
				"outputFileName":    fmt.Sprintf("/workspaces/consequences-runner/data/%v/%v", hydraulicsName, outputName),
				"damage-functions":  "/workspaces/consequences-runner/data/Inland_FFRD_damageFunctions.json",
			},
		},
	}
	err := ComputeFEMAFrequencyEventSingleParameter(a)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}
*/
