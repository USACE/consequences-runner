package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/USACE/go-consequences/structures"
	"github.com/usace/cc-go-sdk"
	filestore "github.com/usace/filestore2"
)

func ConvertGpkgToParquet(geopackageName string) {
	//create a filestore connection to the runs directory
	profile := "FFRD"
	config := filestore.S3FSConfig{
		S3Id:     os.Getenv(fmt.Sprintf("%s_%s", profile, cc.AwsAccessKeyId)),
		S3Key:    os.Getenv(fmt.Sprintf("%s_%s", profile, cc.AwsSecretAccessKey)),
		S3Region: os.Getenv(fmt.Sprintf("%s_%s", profile, cc.AwsDefaultRegion)),
		S3Bucket: os.Getenv(fmt.Sprintf("%s_%s", profile, cc.AwsS3Bucket)),
	}
	fs, err := filestore.NewFileStore(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	//build path to geopackage
	path := fmt.Sprintf("/FFRD_Kanawha_Compute/consequences/%v.gpkg", geopackageName)
	pathconfig := filestore.PathConfig{
		Path: path,
	}
	reader, err := fs.GetObject(pathconfig)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer reader.Close()
	//download locally
	localpath := fmt.Sprintf("/workspaces/consequences-runner/data/%v.gpkg", geopackageName)
	writer, err := os.Create(localpath)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()
	size, err := io.Copy(writer, reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Wrote %v bytes to %v", size, localpath)
	func() {
		//build a gpkg strucure provider
		gsp, err := structureprovider.InitStructureProvider(localpath, geopackageName, "GPKG")
		gsp.SetDeterministic(true)
		if err != nil {
			log.Fatal(err)
		}
		//write out all structures - create a bounding box of the entire united states.

		bbox := geography.BBox{Bbox: []float64{-83, 39, -79, 36}}
		geopackageName = strings.ReplaceAll(geopackageName, " ", "_")
		localparquetpath := fmt.Sprintf("/workspaces/consequences-runner/data/%v", geopackageName)
		rw, err := resultswriters.InitSpatialResultsWriter(localparquetpath, geopackageName, string(resultswriters.PARQUET))
		if err != nil {
			log.Fatal(err)
		}
		defer rw.Close()
		//create a results header that matches the correct data structure of fields.
		s := make([]string, 18)
		s[0] = "fd_id"
		s[1] = "cbfips"
		s[2] = "x"
		s[3] = "y"
		s[4] = "st_damcat"
		s[5] = "occtype"
		s[6] = "val_struct"
		s[7] = "val_cont"
		s[8] = "found_ht"
		s[9] = "found_type"
		s[10] = "num_story"
		s[11] = "pop2amu65"
		s[12] = "pop2amo65"
		s[13] = "pop2pmu65"
		s[14] = "pop2pmo65"
		s[15] = "ground_elv"
		s[16] = "bldgtype"
		s[17] = "firmzone"
		gsp.ByBbox(bbox, func(str consequences.Receptor) {
			// write the str to geoparquet.

			f, ok := str.(structures.StructureDeterministic)
			if !ok {
				return
			}
			data := []interface{}{
				f.Name,
				f.CBFips,
				f.BaseStructure.X,
				f.BaseStructure.Y,
				f.DamCat,
				f.OccType.Name,
				f.StructVal,
				f.ContVal,
				f.FoundHt,
				f.FoundType,
				f.NumStories,
				f.Pop2amu65,
				f.Pop2amo65,
				f.Pop2pmu65,
				f.Pop2pmu65,
				f.GroundElevation,
				f.ConstructionType,
				f.FirmZone,
			}
			result := consequences.Result{
				Headers: s,
				Result:  data,
			}
			rw.Write(result)
		})
	}()
	localparquetpath := fmt.Sprintf("/workspaces/consequences-runner/data/%v", geopackageName)
	parquetbytes, err := os.ReadFile(localparquetpath)
	remoteparquetpath := fmt.Sprintf("/FFRD_Kanawha_Compute/consequences/%v", geopackageName)
	parquetpathconfig := filestore.PathConfig{
		Path: remoteparquetpath,
	}
	fs.PutObject(parquetpathconfig, parquetbytes)
}
