package main

import (
	"encoding/json"
	"fmt"
	"log"

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
	if err != nil {
		log.Fatalf("Terminating the plugin.  Unable to copy structure bytes local : %s\n", err)
	}
	//initalize a structure provider
	sp, err := structureprovider.InitGPK(localStructures, tablename)
	if err != nil {
		log.Fatalf("Terminating the plugin.  Unable to intialize a structure inventory provider : %s\n", err)
	}
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
