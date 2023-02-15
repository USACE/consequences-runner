package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace/cc-go-sdk"
)

func main() {
	fmt.Println("consequences!")
	pm, err := cc.InitPluginManager()
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return
	}
	payload := pm.GetPayload()
	err = computePayload(payload, pm)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		pm.ReportProgress(cc.StatusReport{
			Status:   cc.FAILED,
			Progress: 100,
		})
		return
	}
	pm.ReportProgress(cc.StatusReport{
		Status:   cc.SUCCEEDED,
		Progress: 0,
	})
}
func computePayload(payload cc.Payload, pm *cc.PluginManager) error {

	if len(payload.Outputs) != 1 {
		err := errors.New(fmt.Sprint("expecting one output to be defined found", len(payload.Outputs)))
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	if len(payload.Inputs) != 3 {
		err := errors.New(fmt.Sprint("expecting 2 inputs to be defined found ", len(payload.Inputs)))
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	var gpkgRI cc.DataSource
	var depthGridRI cc.DataSource
	foundDepthGrid := false
	foundGPKG := false
	for _, rfd := range payload.Inputs {
		if strings.Contains(rfd.Name, ".tif") {
			depthGridRI = rfd
			foundDepthGrid = true
		}
		if strings.Contains(rfd.Name, ".gpkg") {
			gpkgRI = rfd
			foundGPKG = true
		}
	}
	if !foundDepthGrid {
		err := fmt.Errorf("could not find tif file for hazard definitions")
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	if !foundGPKG {
		err := fmt.Errorf("could not find .gpkg file for structure inventory")
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	//download the gpkg? can this be virtualized in gdal?
	gpkBytes, err := pm.GetFile(gpkgRI, 0)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	fp := "/app/data/structures.gpkg"
	err = writeLocalBytes(gpkBytes, "/app/data/", fp)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	//initalize a structure provider
	sp, err := structureprovider.InitGPK(fp, "nsi")
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	//initialize a hazard provider
	ds, err := pm.GetStore(depthGridRI.Name)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	var hp hazardproviders.HazardProvider
	if ds.StoreType == cc.S3 {
		hp, err = hazardproviders.Init(fmt.Sprintf("/vsis3/%v", depthGridRI.Paths[0])) //do i need to add vsis3?
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.FATAL,
				Error:      err.Error(),
			})
			return err
		}
	} else {
		tifBytes, err := pm.GetFile(depthGridRI, 0)
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.FATAL,
				Error:      err.Error(),
			})
			return err
		}
		fp := "/app/data/depth.tif"
		err = writeLocalBytes(tifBytes, "/app/data/", fp)
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.FATAL,
				Error:      err.Error(),
			})
			return err
		}
		hp, err = hazardproviders.Init(fp)
	}

	//initalize a results writer
	outfp := "/app/data/result.gpkg"
	rw, err := resultswriters.InitGpkResultsWriter(outfp, "nsi_result")
	//compute results
	compute.StreamAbstract(hp, sp, rw)
	//output read all bytes
	bytes, err := ioutil.ReadFile(outfp)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	for _, datasource := range payload.Outputs {
		if strings.Contains(datasource.Name, ".gpkg") {
			err = pm.PutFile(bytes, datasource, 0)
			if err != nil {
				pm.LogError(cc.Error{
					ErrorLevel: cc.FATAL,
					Error:      err.Error(),
				})
				return err
			}
		}
	}
	return nil
}
func writeLocalBytes(b []byte, destinationRoot string, destinationPath string) error {
	if _, err := os.Stat(destinationRoot); os.IsNotExist(err) {
		os.MkdirAll(destinationRoot, 0644) //do i need to trim filename?
	}
	err := os.WriteFile(destinationPath, b, 0644)
	if err != nil {
		return err
	}
	return nil
}
