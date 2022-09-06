package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"

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
		err := errors.New("expecting one output to be defined found " + len(payload.Outputs))
		logError(err, payload)
		return err
	}
	if len(payload.Inputs) != 3 {
		err := errors.New("expecting 3 inputs to be defined found " + len(payload.Inputs))
		logError(err, payload)
		return err
	}
	var modelResourceInfo plugin.ResourceInfo
	var eventConfigurationResourceInfo plugin.ResourceInfo
	foundModel := false
	foundEventConfig := false
	seedSetName := ""
	for _, rfd := range payload.Inputs {
		if strings.Contains(rfd.FileName, payload.Model.Name+".json") {
			modelResourceInfo = rfd.ResourceInfo
			foundModel = true
		}
		if strings.Contains(rfd.FileName, "eventconfiguration.json") {
			eventConfigurationResourceInfo = rfd.ResourceInfo
			seedSetName = rfd.InternalPaths[0].PathName
			foundEventConfig = true
		}
	}
	if !foundModel {
		err := fmt.Errorf("could not find %s.json", payload.Model.Name)
		logError(err, payload)
		return err
	}
	if !foundEventConfig {
		err := fmt.Errorf("could not find eventconfiguration.json")
		logError(err, payload)
		return err
	}
	modelBytes, err := plugin.DownloadObject(modelResourceInfo)
	if err != nil {
		logError(err, payload)
		return err
	}
	var fcm consequences.Model
	err = json.Unmarshal(modelBytes, &fcm)
	if err != nil {
		logError(err, payload)
		return err
	}
	eventConfiguration, err := plugin.LoadEventConfiguration(eventConfigurationResourceInfo)
	if err != nil {
		logError(err, payload)
		return err
	}
	//then we need to get the specific set of seeds.
	seedSet, err := eventConfiguration.SeedSet(seedSetName)
	if err != nil {
		logError(err, payload)
		return err
	}
	modelResult, err := fcm.Compute(seedSet)
	if err != nil {
		logError(err, payload)
		return err
	}
	bytes, err := json.Marshal(modelResult)
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
