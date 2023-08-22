package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace/cc-go-sdk"
	"github.com/usace/cc-go-sdk/plugin"
)

const (
	tablenameKey            string = "tableName"
	tablenameDefault        string = "nsi"
	structureDatasourceName string = "structures.gpkg"
	seedsDatasourceName     string = "seeds.json"
	depthgridDatasourceName string = "depth-grid"
	localData               string = "/app/data"
	pluginName              string = "consequences"
	outputFileName          string = "results.gpkg"
	outputLayerName         string = "nsi_result"
	outputDatasourceName    string = "Damages Geopackage"
)

func main() {

	pm, err := cc.InitPluginManager()
	if err != nil {
		log.Fatal("Unable to initialize the plugin manager: %s\n", err)
	}
	pl := pm.GetPayload()
	tablename := pl.Attributes.GetStringOrDefault("tableName", tablenameDefault)

	//get structure geopackage
	ds, err := pm.GetInputDataSource(structureDatasourceName)
	if err != nil {
		log.Fatalf("Terminating the plugin.  Unable to get the structures data source : %s\n", err)
	}
	localStructures := fmt.Sprintf("%s/%s", localData, structureDatasourceName)
	err = pm.CopyToLocal(ds, 0, localStructures)

	//initalize a structure provider
	sp, err := structureprovider.InitGPK(localStructures, tablename)
	seedsDs, err := pm.GetInputDataSource(seedsDatasourceName)
	if err != nil {
		log.Println("No seeds provided.  Setting structure provider to deterministic.")
		sp.SetDeterministic(true)
	} else {
		sp.SetDeterministic(false)
		var ec plugin.EventConfiguration
		eventConfigurationReader, err := pm.FileReader(seedsDs, 0)
		if err != nil {
			log.Fatalf("Failed to read seeds from %s: %s\n", seedsDs.Paths[0], err)
		}
		defer eventConfigurationReader.Close()
		err = json.NewDecoder(eventConfigurationReader).Decode(&ec)
		if err != nil {
			log.Fatalf("Invalid seeds.json: %s\n", err)
		}
		seedSet, ssok := ec.Seeds[pluginName]
		if !ssok {
			log.Fatalf("no seeds found by name of %v", pluginName)
		}
		sp.SetSeed(seedSet.EventSeed)
	}

	//initialize a hazard provider
	depthGridDs, err := pm.GetInputDataSource(depthgridDatasourceName)
	if err != nil {
		log.Fatalf("Unable to load the depth grid: %s\n", err)
	}
	depthGridStore, err := pm.GetStore(depthGridDs.StoreName)
	if err != nil {
		log.Fatalf("Invalid depth grid store: %s\n", err)
	}
	var hp hazardproviders.HazardProvider

	if depthGridStore.StoreType == cc.S3 {
		path := fmt.Sprintf("/vsis3/mmc-storage-6%s/%v", depthGridStore.Parameters["root"], depthGridDs.Paths[0])
		hp, err = hazardproviders.Init(path)
		if err != nil {
			log.Fatalf("Failed to initialize hazard provider: %s\n", err)
		}
		defer hp.Close()
	}
	//initalize a results writer
	outfp := fmt.Sprintf("%s/%s", localData, outputFileName)
	var rw consequences.ResultsWriter

	func() {
		rw, err = resultswriters.InitGpkResultsWriter(outfp, outputLayerName)
		if err != nil {
			log.Fatalf("Failed to initilize Geopackage result writer: %s\n", err)
		}
		defer rw.Close()
		//compute results
		compute.StreamAbstract(hp, sp, rw)
	}()

	remoteDs, err := pm.GetOutputDataSource(outputDatasourceName)
	if err != nil {
		log.Fatalf("Unable to load the remote output data source: %s\n", err)
	}
	err = pm.CopyToRemote(outfp, remoteDs, 0)
	if err != nil {
		log.Fatalf("Unable to copy to the remote output data source: %s\n", err)
	}
}

func mainOriginal() {
	fmt.Println(fmt.Sprintf("%v!", pluginName))
	pm, err := cc.InitPluginManager()
	if err != nil {
		log.Fatal(err.Error())
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
		Progress: 100,
	})
}
func computePayload(payload cc.Payload, pm *cc.PluginManager) error {
	tablename := "nsi"
	tableNameObject, ok := payload.Attributes["tableName"]
	if ok {
		tn := tableNameObject.(string)
		tablename = tn
	}
	if len(payload.Outputs) != 1 {
		err := errors.New(fmt.Sprint("expecting one output to be defined found", len(payload.Outputs)))
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	if len(payload.Inputs) < 2 {
		err := errors.New(fmt.Sprint("expecting at least 2 inputs to be defined found ", len(payload.Inputs)))
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	var gpkgRI cc.DataSource
	var depthGridRI cc.DataSource
	var seedsRI cc.DataSource
	foundDepthGrid := false
	foundGPKG := false
	foundSeeds := false
	isVrt := false
	for _, rfd := range payload.Inputs {
		if strings.Contains(rfd.Name, ".tif") {
			depthGridRI = rfd
			foundDepthGrid = true
		}
		if strings.Contains(rfd.Name, ".vrt") {
			depthGridRI = rfd
			foundDepthGrid = true
			isVrt = true
		}
		if strings.Contains(rfd.Name, ".gpkg") {
			gpkgRI = rfd
			foundGPKG = true
		}
		if strings.Contains(rfd.Name, "seeds.json") {
			seedsRI = rfd
			foundSeeds = true
		}
	}
	if !foundDepthGrid {
		err := fmt.Errorf("could not find .tif or .vrt file for hazard definitions")
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
	sp, err := structureprovider.InitGPK(fp, tablename)
	if foundSeeds {
		sp.SetDeterministic(false)
		var ec plugin.EventConfiguration
		eventConfigurationReader, err := pm.FileReader(seedsRI, 0)
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.ERROR,
				Error:      err.Error(),
			})
			return err
		}
		defer eventConfigurationReader.Close()
		err = json.NewDecoder(eventConfigurationReader).Decode(&ec)
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.ERROR,
				Error:      err.Error(),
			})
			return err
		}

		seedSetName := pluginName
		seedSet, ssok := ec.Seeds[seedSetName]
		if !ssok {
			pm.LogError(cc.Error{
				ErrorLevel: cc.ERROR,
				Error:      fmt.Errorf("no seeds found by name of %v", seedSetName).Error(),
			})
			return err
		}
		//fmt.Print(seedSet)
		sp.SetSeed(seedSet.EventSeed)
	} else {
		sp.SetDeterministic(true)
	}

	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	//initialize a hazard provider
	ds, err := pm.GetStore(depthGridRI.StoreName)
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	var hp hazardproviders.HazardProvider
	///vsis3/mmc-storage-6/model-library/FFRD_Kanawha_Compute/common-files/QcTests/upper-kanawha/grids-p01
	if ds.StoreType == cc.S3 {
		path := fmt.Sprintf("/vsis3/%v", depthGridRI.Paths[0])
		if isVrt {
			for _, p := range depthGridRI.Paths {
				if strings.Contains(p, ".vrt") {
					path = fmt.Sprintf("/vsis3/mmc-storage-6%s/%v", ds.Parameters["root"], p)
				}
			}
		}
		hp, err = hazardproviders.Init(path) //do i need to add vsis3?
		if err != nil {
			pm.LogError(cc.Error{
				ErrorLevel: cc.FATAL,
				Error:      err.Error(),
			})
			return err
		}
		defer hp.Close()
	} else {
		if isVrt {
			pm.LogError(cc.Error{
				ErrorLevel: cc.FATAL,
				Error:      "vrt files should be accessed directly from s3, please update your payload.",
			})
			return err
		}
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
		defer hp.Close()
	}

	//initalize a results writer
	outfp := "/app/data/result.gpkg"
	rw, err := resultswriters.InitGpkResultsWriter(outfp, "nsi_result")
	if err != nil {
		pm.LogError(cc.Error{
			ErrorLevel: cc.FATAL,
			Error:      err.Error(),
		})
		return err
	}
	defer rw.Close()
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
		if datasource.Name == "Damages Geopackage" {
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
