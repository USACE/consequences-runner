package main

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

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
