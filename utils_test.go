package main

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

/*
	func Test_ConvertParquet(t *testing.T) {
		//ConvertGpkgToParquet("Bluestone Local")
		ConvertGpkgToParquet("Bluestone Upper")
		ConvertGpkgToParquet("Coal")
		ConvertGpkgToParquet("Elk Middle")
		ConvertGpkgToParquet("Elk at Sutton")
		ConvertGpkgToParquet("Gauley Lower")
		ConvertGpkgToParquet("Gauley at Summersville")
		ConvertGpkgToParquet("Greenbrier")
		ConvertGpkgToParquet("Lower Kanawha-Elk Lower")
		ConvertGpkgToParquet("Lower New")
		ConvertGpkgToParquet("New Middle")
		ConvertGpkgToParquet("New-Little River")
		ConvertGpkgToParquet("Upper Kanawha")
		ConvertGpkgToParquet("Upper New at Claytor")
	}
*/
func Test_Main(t *testing.T) {
	main()
}
func Test_Logic(t *testing.T) {
	inventoryDriver := "SHP"
	result := strings.Compare(inventoryDriver, "GPKG")
	fmt.Println(result)
	result2 := strings.Compare(inventoryDriver, "JSON")
	fmt.Println(result2)
	if strings.Compare(inventoryDriver, "GPKG") == 0 || strings.Compare(inventoryDriver, "JSON") == 0 {
		log.Fatal("Terminating the plugin.  Only GPKG, SHP or PARQUET drivers support at this time\n")
	}
}

/*
func Test_Download(t *testing.T) {

	remote_root := "/model-library/ffrd-duwamish/scenario-complex-levees/simulations/"
	local_root := "/workspaces/consequences-runner/data/scenario-complex-levees/simulations/"
	objects := []string{
		"%v/consequences/duwamish/Duwamish_NSIv2022_Calibrated_consequences.gpkg",
	}
	remote_objects := []string{}
	local_objects := []string{}
	for r := 1; r < 5136; r++ {
		for _, f := range objects {
			o := fmt.Sprintf(f, r)
			remote := fmt.Sprintf("%v%v", remote_root, o)
			local := fmt.Sprintf("%v%v", local_root, o)
			remote_objects = append(remote_objects, remote)
			local_objects = append(local_objects, local)
		}
	}

	Download(remote_objects, local_objects)
}
*/
