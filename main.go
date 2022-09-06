package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/hazardproviders"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/USACE/go-consequences/structureprovider"
	"github.com/usace/wat-go-sdk/plugin"
)

func main() {
	fmt.Println("consequences!")
	var payloadPath string
	flag.StringVar(&payloadPath, "payload", "pathtopayload.yml", "please specify an input file using `-payload pathtopayload.yml`")
	flag.Parse()
	if payloadPath == "" {
		plugin.Log(plugin.Message{
			Status:    plugin.FAILED,
			Progress:  0,
			Level:     plugin.ERROR,
			Message:   "given a blank path...\n\tplease specify an input file using `payload pathtopayload.yml`",
			Sender:    "go-consequences-wat",
			PayloadId: "unknown payloadid because the plugin package could not be properly initalized",
		})
		return
	}
	err := plugin.InitConfigFromEnv()
	if err != nil {
		logError(err, plugin.ModelPayload{Id: "unknownpayloadid"})
		return
	}
	payload, err := plugin.LoadPayload(payloadPath)
	if err != nil {
		logError(err, plugin.ModelPayload{Id: "unknownpayloadid"})
		return
	}
	err = computePayload(payload)
	if err != nil {
		logError(err, payload)
		return
	}
}
func computePayload(payload plugin.ModelPayload) error {

	if len(payload.Outputs) != 1 {
		err := errors.New(fmt.Sprint("expecting one output to be defined found", len(payload.Outputs)))
		logError(err, payload)
		return err
	}
	if len(payload.Inputs) != 3 {
		err := errors.New(fmt.Sprint("expecting 2 inputs to be defined found ", len(payload.Inputs)))
		logError(err, payload)
		return err
	}
	var gpkgRI plugin.ResourceInfo
	var depthGridRI plugin.ResourceInfo
	foundDepthGrid := false
	foundGPKG := false
	for _, rfd := range payload.Inputs {
		if strings.Contains(rfd.FileName, ".tif") {
			depthGridRI = rfd.ResourceInfo
			foundDepthGrid = true
		}
		if strings.Contains(rfd.FileName, ".gpkg") {
			gpkgRI = rfd.ResourceInfo
			foundGPKG = true
		}
	}
	if !foundDepthGrid {
		err := fmt.Errorf("could not find tif file for hazard definitions")
		logError(err, payload)
		return err
	}
	if !foundGPKG {
		err := fmt.Errorf("could not find .gpkg file for structure inventory")
		logError(err, payload)
		return err
	}
	//download the gpkg? can this be virtualized in gdal?
	gpkBytes, err := plugin.DownloadObject(gpkgRI)
	if err != nil {
		logError(err, payload)
		return err
	}
	fp := "/app/data/structures.gpkg"
	err = writeLocalBytes(gpkBytes, "/app/data/", fp)
	if err != nil {
		logError(err, payload)
		return err
	}
	//initalize a structure provider
	sp, err := structureprovider.InitGPK(fp, "nsi")
	if err != nil {
		logError(err, payload)
		return err
	}
	//initialize a hazard provider
	hp, err := hazardproviders.Init(fmt.Sprintf("/vsis3/%v", depthGridRI.Path)) //do i need to add vsis3?
	if err != nil {
		logError(err, payload)
		return err
	}
	//initalize a results writer
	outfp := "/app/data/result.gpkg"
	rw, err := resultswriters.InitGpkResultsWriter(outfp, "nsi_result")
	//compute results
	compute.StreamAbstract(hp, sp, rw)
	//output read all bytes
	bytes, err := ioutil.ReadFile(outfp)
	if err != nil {
		logError(err, payload)
		return err
	}
	err = plugin.UpLoadFile(payload.Outputs[0].ResourceInfo, bytes)
	if err != nil {
		logError(err, payload)
		return err
	}
	plugin.Log(plugin.Message{
		Status:    plugin.SUCCEEDED,
		Progress:  100,
		Level:     plugin.INFO,
		Message:   "consequences complete",
		Sender:    "go-consequences-wat",
		PayloadId: payload.Id,
	})
	return nil
}
func logError(err error, payload plugin.ModelPayload) {
	plugin.Log(plugin.Message{
		Status:    plugin.FAILED,
		Progress:  0,
		Level:     plugin.ERROR,
		Message:   err.Error(),
		Sender:    "go-consequences-wat",
		PayloadId: payload.Id,
	})
}
func writeLocalBytes(b []byte, destinationRoot string, destinationPath string) error {
	if _, err := os.Stat(destinationRoot); os.IsNotExist(err) {
		os.MkdirAll(destinationRoot, 0644) //do i need to trim filename?
	}
	err := os.WriteFile(destinationPath, b, 0644)
	if err != nil {
		plugin.Log(plugin.Message{
			Message: fmt.Sprintf("failure to write local file: %v\n\terror:%v", destinationPath, err),
			Level:   plugin.ERROR,
			Sender:  "go-consequences-wat",
		})
		return err
	}
	return nil
}
