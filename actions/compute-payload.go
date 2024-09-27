package actions

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/USACE/go-consequences/structures"
	"github.com/usace/cc-go-sdk"
)

const (
	tablenameKey string = "tableName" //plugin attribute key required
	//studyAreaKey               string = "studyArea"            // plugin attribute key required - describes what seedset to use.
	bucketKey                  string = "bucket"               //plugin attribute key required - bucket only. i.e. mmc-storage-6 - will be combined with datastore root parameter
	inventoryDriverKey         string = "inventoryDriver"      //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputDriverKey            string = "outputDriver"         //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputFileNameKey          string = "outputFileName"       //plugin attribute key required should include extension compatable with driver name.
	useKnowledgeUncertaintyKey string = "knowledgeUncertainty" //plugin attribute key required -
	outputLayerName            string = "damages"
	structureInventoryPathKey  string = "Inventory" //plugin datasource name required
	//seedsDatasourceName        string = "seeds.json" //plugin datasource name required
	depthgridDatasourceName string = "depth-grid" //plugin datasource name required
	outputDatasourceName    string = "Damages"    //plugin output datasource name required
	localData               string = "/app/data"
	pluginName              string = "consequences"
	DepthGridPathsKey       string = "depth-grids"      // expected to contain the fully qualified vsis3 path set comma separated or the local path if the resource is included as an inputdatasource
	VelocityGridPathsKey    string = "velocity-grids"   // expected to contain the fully qualified vsis3 path set comma separated or the local path if the resource is included as an inputdatasource
	FrequenciesKey          string = "frequencies"      //expected to be comma separated string
	inventoryPathKey        string = "Inventory"        //expected this is local - needs to agree with the payload input datasource name
	damageFunctionPathKey   string = "damage-functions" //expected this is local - needs to agree with the payload input datasource name
)

func CopyInputs(pl cc.Payload, pm *cc.PluginManager) {
	for _, i := range pl.Inputs {
		pm.CopyToLocal(i, 0, fmt.Sprintf("%v/%v", localData, i.Name))
	}
}
func PostOutputs(pl cc.Payload, pm *cc.PluginManager) {
	extension := ""
	for _, o := range pl.Outputs {
		for i, dp := range o.Paths {
			extension = filepath.Ext(dp)
			pm.CopyToRemote(fmt.Sprintf("%v/%v%v", localData, o.Name, extension), o, i)
		}

	}
}
func ComputeEvent(a cc.Action) error {
	tablename := a.Parameters.GetStringOrFail(tablenameKey)
	InventoryPath := a.Parameters.GetStringOrFail(structureInventoryPathKey)
	inventoryDriver := a.Parameters.GetStringOrFail(inventoryDriverKey)
	outputDriver := a.Parameters.GetStringOrFail(outputDriverKey)
	outputFileName := a.Parameters.GetStringOrFail(outputFileNameKey)
	depthHazardPath := a.Parameters.GetStringOrFail(depthgridDatasourceName)
	//get structure inventory (assumed local or path is defined as vsis3)
	//initalize a structure provider
	sp, err := structureprovider.InitStructureProvider(InventoryPath, tablename, inventoryDriver)

	if err != nil {
		log.Fatalf("Terminating the plugin.  Unable to intialize a structure inventory provider : %s\n", err)
	}
	sp.SetDeterministic(true)

	//initialize a hazard provider
	var hp hazardproviders.HazardProvider
	hp, err = hazardproviders.Init_CustomFunction(depthHazardPath, func(valueIn hazards.HazardData, hazard hazards.HazardEvent) (hazards.HazardEvent, error) {
		if valueIn.Depth == 0 {
			return hazard, hazardproviders.NoHazardFoundError{}
		}
		process := hazardproviders.DepthHazardFunction()
		return process(valueIn, hazard)
	})

	//initalize a results writer
	outfp := fmt.Sprintf("%s/%s", localData, outputFileName)
	projected, ok := hp.(geography.Projected)

	var rw consequences.ResultsWriter
	if ok {
		sr := projected.SpatialReference()
		rw, err = resultswriters.InitSpatialResultsWriter_WKT_Projected(outfp, outputLayerName, outputDriver, sr)
		if err != nil {
			log.Fatalf("Failed to initilize spatial result writer: %s\n", err)
		}
	} else {
		//could be dangerous
		log.Printf("hazard provider does not implement geography.projected, results may not be reasonable assumed 4326")
		rw, err = resultswriters.InitSpatialResultsWriter(outfp, outputLayerName, outputDriver)
		if err != nil {
			log.Fatalf("Failed to initilize spatial result writer: %s\n", err)
		}
	}
	defer rw.Close()
	//compute results
	compute.StreamAbstract(hp, sp, rw)
	return nil
}
func ComputeFrequencyEvent(a cc.Action) error {
	// get all relevant parameters
	tablename := a.Parameters.GetStringOrFail(tablenameKey)
	//vsis3prefix := a.Parameters.GetStringOrFail(vsis3prefixKey)
	depthGridPathString := a.Parameters.GetStringOrFail(DepthGridPathsKey)       // expected this is a vsis3 object
	velocityGridPathString := a.Parameters.GetStringOrFail(VelocityGridPathsKey) // expected this is a vsis3 object
	//durationGridPaths := a.Parameters.GetStringOrFail(DurationGridPathsKey)// expected this is a vsis3 object
	frequencystring := a.Parameters.GetStringOrFail(FrequenciesKey)
	inventoryPathKey := a.Parameters.GetStringOrFail(inventoryPathKey) //expected this is local - needs to agree with the payload input datasource name
	inventoryDriver := a.Parameters.GetStringOrFail(inventoryDriverKey)

	outputDriver := a.Parameters.GetStringOrFail(outputDriverKey)
	outputFileName := a.Parameters.GetStringOrFail(outputFileNameKey) //expected this is local - needs to agree with the payload output datasource name
	//useKnowledgeUncertainty, err := strconv.ParseBool(a.Parameters.GetStringOrFail(useKnowledgeUncertaintyKey))
	damageFunctionPath := a.Parameters.GetStringOrFail(damageFunctionPathKey) //expected this is local - needs to agree with the payload input datasource name
	// frequencies expected to be comma separated variables of floats.
	stringFrequencies := strings.Split(frequencystring, ", ")
	frequencies := make([]float64, 0)
	for _, s := range stringFrequencies {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		frequencies = append(frequencies, f)
	}
	// grid paths expected to be comma separated variables of string path parts
	DepthGridPaths := strings.Split(depthGridPathString, ", ")
	VelocityGridPaths := strings.Split(velocityGridPathString, ", ")
	if len(DepthGridPaths) != len(VelocityGridPaths) {
		return errors.New("depth grids and velocity grids have different numbers of paths")
	}
	if len(DepthGridPaths) != len(frequencies) {
		return errors.New("hazard grids have different numbers of paths than the frequencies list")
	}
	hps := make([]hazardproviders.HazardProvider, 0)
	for i, dp := range DepthGridPaths {
		hpi := hazardproviders.HazardProviderInfo{
			Hazards: []hazardproviders.HazardProviderParameterAndPath{{
				Hazard:   hazards.Depth,
				FilePath: dp,
			}, {
				Hazard:   hazards.Velocity,
				FilePath: VelocityGridPaths[i],
			}},
		}
		hp, err := hazardproviders.InitMulti(hpi)
		if err != nil {
			return err
		}
		hps = append(hps, hp)
	}
	// inventory path expected to be a local path
	// damage function path expected to be a local path
	sp, err := structureprovider.InitStructureProviderwithOcctypePath(inventoryPathKey, tablename, inventoryDriver, damageFunctionPath)
	sp.SetDeterministic(true)
	if err != nil {
		return err
	}
	fmt.Sprintln(sp.FilePath)
	//results writer
	outfp := outputFileName //fmt.Sprintf("%s/%s", localData, outputFileName)
	var rw consequences.ResultsWriter
	sr := sp.SpatialReference()
	rw, err = resultswriters.InitSpatialResultsWriter_WKT_Projected(outfp, outputLayerName, outputDriver, sr)
	if err != nil {
		return err
	}
	defer rw.Close()

	ComputeMultiFrequency(hps, frequencies, sp, rw)
	return nil
}
func ComputeMultiFrequency(hps []hazardproviders.HazardProvider, freqs []float64, sp consequences.StreamProvider, w consequences.ResultsWriter) {
	fmt.Printf("Computing %v frequencies\n", len(freqs))
	//ASSUMPTION hazard providers and frequencies are in the same order
	//ASSUMPTION ordered by most frequent to least frequent event
	//ASSUMPTION! get bounding box from largest frequency.

	largestHp := hps[len(hps)-1]
	bbox, err := largestHp.HazardBoundary()
	if err != nil {
		fmt.Print(err)
		return
	}
	//set up output tables for all frequencies.
	header := []string{"ORIG_ID", "REPVAL", "STORY", "FOUND_T", "FOUND_H", "x", "y", "OccType", "DamCat", "BASEFIN", "FFH", "DEMFT", "BAAL", "CAAL", "TAAL", "PROB"}

	for _, f := range freqs {
		header = append(header, fmt.Sprintf("%2.6fS", f))
		header = append(header, fmt.Sprintf("%2.6fC", f))
		header = append(header, fmt.Sprintf("%2.6fH", f))
	}

	sp.ByBbox(bbox, func(f consequences.Receptor) {
		s, sok := f.(structures.StructureDeterministic)
		if !sok {
			return
		}
		results := []interface{}{s.Name, s.StructVal, s.NumStories, s.FoundType, s.FoundHt, s.Location().X, s.Location().Y, s.OccType.Name, s.DamCat, "unkown", s.FoundHt + s.GroundElevation, s.GroundElevation, 0.0, 0.0, 0.0, 0.0}

		sEADs := make([]float64, len(freqs))
		cEADs := make([]float64, len(freqs))
		hazarddata := make([]hazards.HazardEvent, len(freqs))
		//ProvideHazard works off of a geography.Location
		gotWet := false
		firstProb := 0.0
		for index, hp := range hps {
			d, err := hp.Hazard(geography.Location{X: f.Location().X, Y: f.Location().Y})
			hazarddata = append(hazarddata, d)
			//compute damages based on hazard being able to provide depth

			if err == nil {
				r, err3 := f.Compute(d)
				if err3 == nil {
					if !gotWet {
						firstProb = freqs[index]
					}
					gotWet = true
					sdam, err := r.Fetch("structure damage")
					if err != nil {
						//panic?
						sEADs[index] = 0.0
					} else {
						damage := sdam.(float64)
						sEADs[index] = damage
					}
					cdam, err := r.Fetch("content damage")
					if err != nil {
						//panic?
						cEADs[index] = 0.0
					} else {
						damage := cdam.(float64)
						cEADs[index] = damage
					}
				}
				results = append(results, sEADs[index])
				results = append(results, cEADs[index])
				b, err := json.Marshal(d)
				if err != nil {
					log.Fatal(err)
				}
				shaz := string(b)
				results = append(results, shaz)
			} else {
				//record zeros?
				results = append(results, 0.0)
				results = append(results, 0.0)
				results = append(results, "no hazard")
			}
		}
		results[15] = firstProb
		sEAD := compute.ComputeSpecialEAD(sEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[12] = sEAD
		cEAD := compute.ComputeSpecialEAD(cEADs, freqs) //use compute special ead to not create triangle below the most frequent event
		results[13] = cEAD
		results[14] = sEAD + cEAD
		var ret = consequences.Result{Headers: header, Result: results}
		if gotWet {
			w.Write(ret)
		}

	})

}
