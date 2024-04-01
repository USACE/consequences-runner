package actions

import (
	"github.com/USACE/go-consequences/compute"
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/hazardproviders"
)

func Compute(hp hazardproviders.HazardProvider, sp consequences.StreamProvider, rw consequences.ResultsWriter) error {
	defer rw.Close()
	//compute results
	compute.StreamAbstract(hp, sp, rw)
	return nil
}
