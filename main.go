package main

import (
	"log"
	"os"
	"time"

	"github.com/usace/cc-go-sdk"
	"github.com/usace/consequences-runner/actions"
)

const (
	localData  string = "/app/data"
	pluginName string = "consequences"
)

func main() {
	log.Println(time.Now())
	t := time.Now()
	//make sure a local directory exists
	if _, err := os.Stat(localData); os.IsNotExist(err) {
		os.MkdirAll(localData, 0644) //do i need to trim filename?
	}
	pm, err := cc.InitPluginManager()
	if err != nil {
		log.Fatalf("Unable to initialize the plugin manager: %s\n", err.Error())
	}
	pl := pm.Payload
	for _, a := range pl.Actions {
		switch a.Type {
		case "compute-event":
			actions.ComputeEvent(a)
			break
		case "compute-event-chart":
			actions.ComputeEventChart(a)
			break
		case "compute-frequency":
			actions.ComputeFrequencyEvent(a)
			break
		case "compute-fema-frequency":
			actions.ComputeFEMAFrequencyEvent(a)
			break
		case "copy-inputs":
			actions.CopyInputs(pl, pm)
			break
		case "post-outputs":
			actions.PostOutputs(pl, pm)
			break
		}
	}
	log.Println(time.Now())
	s := time.Now().Sub(t)
	log.Println(s)
}
