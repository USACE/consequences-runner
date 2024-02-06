package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace/cc-go-sdk"
	"github.com/usace/cc-go-sdk/plugin"
)

const (
	tablenameKey            string = "tableName"       //plugin attribute key required
	bucketKey               string = "bucket"          //plugin attribute key required - bucket only. i.e. mmc-storage-6 - will be combined with datastore root parameter
	inventoryDriverKey      string = "inventoryDriver" //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputDriverKey         string = "outputDriver"    //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputFileNameKey       string = "outputFileName"  //plugin attribute key required should include extension compatable with driver name.
	outputLayerName         string = "damages"
	structureDatasourceName string = "Inventory"  //plugin datasource name required
	seedsDatasourceName     string = "seeds.json" //plugin datasource name required
	depthgridDatasourceName string = "depth-grid" //plugin datasource name required
	outputDatasourceName    string = "Damages"    //plugin output datasource name required
	localData               string = "/app/data"
	pluginName              string = "consequences"
)

func main() {

	pm, err := cc.InitPluginManager()
	if err != nil {
		log.Fatal("Unable to initialize the plugin manager: %s\n", err)
	}
	pl := pm.GetPayload()
	tablename := pl.Attributes.GetStringOrFail(tablenameKey)
	bucketname := pl.Attributes.GetStringOrFail(bucketKey)
	inventoryDriver := pl.Attributes.GetStringOrFail(inventoryDriverKey)
	outputDriver := pl.Attributes.GetStringOrFail(outputDriverKey)
	outputFileName := pl.Attributes.GetStringOrFail(outputFileNameKey)

	//get structure inventory datasource
	ds, err := pm.GetInputDataSource(structureDatasourceName)
	if err != nil {
		log.Fatalf("Terminating the plugin.  Unable to get the structures data source : %s\n", err)
	}
	fp := ds.Paths[0]
	if inventoryDriver != "PARQUET" {
		if inventoryDriver != "GPKG" || inventoryDriver != "JSON" {
			log.Fatalf("Terminating the plugin.  Only GPKG, SHP or PARQUET drivers support at this time\n", err)
		}
		localStructures := fmt.Sprintf("%s/%s", localData, structureDatasourceName)
		err = pm.CopyToLocal(ds, 0, localStructures)
		if err != nil {
			log.Fatalf("Terminating the plugin.  Unable to copy structure bytes local : %s\n", err)
		}
		fp = localStructures
	} else {
		dsStore, err := pm.GetStore(ds.StoreName)
		if err != nil {
			log.Fatalf("Terminating the plugin.  Unable to to remote structure bytes : %s\n", err)
		}
		fp = fmt.Sprintf("/vsis3/%s%s/%v", bucketname, dsStore.Parameters["root"], ds.Paths[0])
	}

	//initalize a structure provider
	sp, err := structureprovider.InitStructureProvider(fp, tablename, inventoryDriver)
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
		path := fmt.Sprintf("/vsis3/%s%s/%v", bucketname, depthGridStore.Parameters["root"], depthGridDs.Paths[0])
		hp, err = hazardproviders.Init_CustomFunction(path, func(valueIn hazards.HazardData, hazard hazards.HazardEvent) (hazards.HazardEvent, error) {
			if valueIn.Depth == 0 {
				return hazard, hazardproviders.NoHazardFoundError{}
			}
			process := hazardproviders.DepthHazardFunction()
			return process(valueIn, hazard)
		})
		if err != nil {
			log.Fatalf("Failed to initialize hazard provider: %s\n", err)
		}
		defer hp.Close()
	}
	//initalize a results writer
	outfp := fmt.Sprintf("%s/%s", localData, outputFileName)
	var rw consequences.ResultsWriter

	func() {
		rw, err = resultswriters.InitSpatialResultsWriter(outfp, outputLayerName, outputDriver)
		if err != nil {
			log.Fatalf("Failed to initilize spatial result writer: %s\n", err)
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
