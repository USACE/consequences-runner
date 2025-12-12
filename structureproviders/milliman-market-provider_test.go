package structureproviders

import (
	"fmt"
	"testing"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/hazards"
	"github.com/USACE/go-consequences/structures"
)

func Test_Load(t *testing.T) {
	sp, err := InitStructureProvider("/workspaces/consequences-runner/data/DC_ucmb.csv")
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	counter := 0
	sp.processBboxStreamDeterministic(geography.BBox{[]float64{0, 0, 0, 0.0}}, func(s consequences.Receptor) {
		counter++
		st, ok := s.(structures.StructureStochastic)
		if ok {
			fmt.Println(st.OccType.ComponentDamageFunctions["structure"].DamageFunctions[hazards.Depth].Source)
		}

	})
	if counter != 101 {
		t.Errorf("yeilded %d structures; expected 101", counter)
	}
}
