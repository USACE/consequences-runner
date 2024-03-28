package main

import "testing"

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
