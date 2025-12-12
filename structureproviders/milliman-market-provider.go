package structureproviders

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/geography"
	"github.com/USACE/go-consequences/structures"
	"github.com/dewberry/gdal"
)

type gdalDataSet struct {
	FilePath              string
	LayerName             string
	schemaIDX             []int
	ds                    *gdal.DataSource
	deterministic         bool
	seed                  int64
	OccTypeProvider       structures.OccupancyTypeProvider
	FoundationUncertainty *structures.FoundationUncertainty
}

func StructureSchema() []string {
	s := make([]string, 10)
	s[0] = "accntnum"
	s[1] = "LAT"
	s[2] = "LON"
	s[3] = "BLDG_VALUE"
	s[4] = "CNT_VALUE"
	s[5] = "FoundationType"
	s[6] = "NUM_STORIES"
	s[7] = "elev_ft"
	s[8] = "CONSTR_CODE"
	s[9] = "FIRST_FLOOR_ELEV"
	return s
}

func OptionalSchema() []string {
	s := make([]string, 8)

	return s
}
func InitStructureProvider(filepath string) (*gdalDataSet, error) {
	//validation?
	gpk, err := initalizestructureprovider(filepath)
	gpk.setOcctypeProvider(false, "")
	gpk.UpdateFoundationHeightUncertainty(false, "")
	return &gpk, err
}
func InitStructureProviderwithOcctypePath(filepath string, occtypefp string) (*gdalDataSet, error) {
	//validation?
	gpk, err := initalizestructureprovider(filepath)
	gpk.setOcctypeProvider(true, occtypefp)
	return &gpk, err
}
func (ds *gdalDataSet) UpdateFoundationHeightUncertainty(useFile bool, foundationHeightUncertaintyJsonFilePath string) {
	if useFile {
		fh, err := structures.InitFoundationUncertaintyFromFile(foundationHeightUncertaintyJsonFilePath)
		if err != nil {
			fh, _ = structures.InitFoundationUncertainty()
		}
		ds.FoundationUncertainty = fh
	} else {
		fh, _ := structures.InitFoundationUncertainty()
		ds.FoundationUncertainty = fh
	}
}
func initalizestructureprovider(filepath string) (gdalDataSet, error) {
	driverOut := gdal.OGRDriverByName("CSV")
	ds, dsok := driverOut.Open(filepath, int(gdal.ReadOnly))
	if !dsok {
		return gdalDataSet{}, errors.New("error opening structure provider of type CSV")
	}

	l := ds.LayerByName(ds.LayerByIndex(0).Name())
	def := l.Definition()
	s := StructureSchema()
	sIDX := make([]int, len(s))
	for i, f := range s {
		idx := def.FieldIndex(f)
		if idx < 0 {
			return gdalDataSet{}, errors.New("gdal dataset at path " + filepath + " Expected field named " + f + " none was found")
		}
		sIDX[i] = idx
	}
	gpk := gdalDataSet{FilePath: filepath, LayerName: ds.LayerByIndex(0).Name(), schemaIDX: sIDX, ds: &ds, seed: 1234}
	return gpk, nil
}
func (gpk *gdalDataSet) setOcctypeProvider(useFilepath bool, filepath string) {
	if useFilepath {
		otp := structures.JsonOccupancyTypeProvider{}
		otp.InitLocalPath(filepath)
		gpk.OccTypeProvider = otp
	} else {
		otp := structures.JsonOccupancyTypeProvider{}
		otp.InitDefault()
		gpk.OccTypeProvider = otp
	}
}
func (gpk *gdalDataSet) SetDeterministic(useDeterministic bool) {
	gpk.deterministic = useDeterministic
}
func (gpk *gdalDataSet) SetSeed(seed int64) {
	gpk.seed = seed
}
func (gpk *gdalDataSet) SpatialReference() string {
	l := gpk.ds.LayerByName(gpk.LayerName)
	sr := l.SpatialReference()
	wkt, err := sr.ToWKT()
	if err != nil {
		return ""
	}
	return wkt
}
func (gpk *gdalDataSet) UpdateSpatialReference(sr_wkt string) {
	// unimplemented
	fmt.Println("could not set spatial reference")
}

// StreamByFips a streaming service for structure stochastic based on a bounding box
func (gpk gdalDataSet) ByFips(fipscode string, sp consequences.StreamProcessor) {
	if gpk.deterministic {
		gpk.processFipsStreamDeterministic(fipscode, sp)
	} else {
		gpk.processFipsStream(fipscode, sp)
	}

}
func (gpk gdalDataSet) processFipsStream(fipscode string, sp consequences.StreamProcessor) {
	m := gpk.OccTypeProvider.OccupancyTypeMap()
	//define a default occtype in case of emergancy
	defaultOcctype := m["RES1-1SNB"]
	idx := 0
	l := gpk.ds.LayerByName(gpk.LayerName)
	fc, _ := l.FeatureCount(true)
	r := rand.New(rand.NewSource(gpk.seed))
	for idx < fc { // Iterate and fetch the records from result cursor
		f := l.NextFeature()
		idx++
		if f != nil {
			s, err := featuretoStructure(f, m, defaultOcctype, gpk.schemaIDX)
			s.ApplyFoundationHeightUncertanty(gpk.FoundationUncertainty)
			s.UseUncertainty = true
			sd := s.SampleStructure(r.Int63())
			if err == nil {
				sp(sd)
			}
		}
	}
}
func (gpk gdalDataSet) processFipsStreamDeterministic(fipscode string, sp consequences.StreamProcessor) {
	m := gpk.OccTypeProvider.OccupancyTypeMap()
	m2 := swapOcctypeMap(m)
	//define a default occtype in case of emergancy
	defaultOcctype := m2["RES1-1SNB"]
	idx := 0
	l := gpk.ds.LayerByName(gpk.LayerName)
	fc, _ := l.FeatureCount(true)
	for idx < fc { // Iterate and fetch the records from result cursor
		f := l.NextFeature()
		idx++
		if f != nil {
			s, err := featuretoDeterministicStructure(f, m2, defaultOcctype, gpk.schemaIDX)
			if err == nil {
				sp(s)
			}
		}
	}
}
func (gpk gdalDataSet) ByBbox(bbox geography.BBox, sp consequences.StreamProcessor) {
	if gpk.deterministic {
		gpk.processBboxStreamDeterministic(bbox, sp)
	} else {
		gpk.processBboxStream(bbox, sp)
	}

}
func (gpk gdalDataSet) processBboxStream(bbox geography.BBox, sp consequences.StreamProcessor) {
	m := gpk.OccTypeProvider.OccupancyTypeMap()
	//define a default occtype in case of emergancy
	defaultOcctype := m["RES1-1SNB"]
	idx := 0
	l := gpk.ds.LayerByName(gpk.LayerName)
	l.SetSpatialFilterRect(bbox.Bbox[0], bbox.Bbox[3], bbox.Bbox[2], bbox.Bbox[1])
	fc, _ := l.FeatureCount(true)
	r := rand.New(rand.NewSource(gpk.seed))
	for idx < fc { // Iterate and fetch the records from result cursor
		f := l.NextFeature()
		idx++
		if f != nil {
			s, err := featuretoStructure(f, m, defaultOcctype, gpk.schemaIDX)
			s.ApplyFoundationHeightUncertanty(gpk.FoundationUncertainty)
			s.UseUncertainty = true
			sd := s.SampleStructure(r.Int63())
			if err == nil {
				sp(sd)
			}
		}
	}
}

func (gpk gdalDataSet) processBboxStreamDeterministic(bbox geography.BBox, sp consequences.StreamProcessor) {
	m := gpk.OccTypeProvider.OccupancyTypeMap()
	m2 := swapOcctypeMap(m)
	//define a default occtype in case of emergancy
	defaultOcctype := m2["RES1-1SNB"]
	idx := 0
	l := gpk.ds.LayerByName(gpk.LayerName)
	l.SetSpatialFilterRect(bbox.Bbox[0], bbox.Bbox[3], bbox.Bbox[2], bbox.Bbox[1])
	fc, _ := l.FeatureCount(true)
	for idx < fc { // Iterate and fetch the records from result cursor
		f := l.NextFeature()
		idx++
		if f != nil {
			s, err := featuretoDeterministicStructure(f, m2, defaultOcctype, gpk.schemaIDX)
			if err == nil {
				sp(s)
			}
		}
	}
}

func featuretoStructure(
	f *gdal.Feature,
	m map[string]structures.OccupancyTypeStochastic,
	defaultOcctype structures.OccupancyTypeStochastic,
	idxs []int,
) (structures.StructureStochastic, error) {
	defer f.Destroy()
	s := structures.StructureStochastic{}
	s.Name = fmt.Sprintf("%v", f.FieldAsInteger(idxs[0]))
	basementstring := "N"
	if f.FieldAsInteger(idxs[9]) == 2 {
		basementstring = "W"
	}
	OccTypeName := fmt.Sprintf("RES1-%dS%vB", f.FieldAsInteger(idxs[6]), basementstring) //f.FieldAsString(idxs[5])
	var occtype = defaultOcctype
	//dont have access to foundation type in the structure schema yet.
	if idxs[9] > 0 {
		if otf, okf := m[OccTypeName+"-"+f.FieldAsString(idxs[9])]; okf {
			occtype = otf
		} else {
			if ot, ok := m[OccTypeName]; ok {
				occtype = ot
			} else {
				occtype = defaultOcctype
				msg := "Using default " + OccTypeName + " not found"
				fmt.Println(msg)
				//return s, errors.New(msg)
			}
		}
	} else {
		if ot, ok := m[OccTypeName]; ok {
			occtype = ot
		} else {
			occtype = defaultOcctype
			msg := "Using default " + OccTypeName + " not found"
			fmt.Println(msg)
			//return s, errors.New(msg)
		}
	}

	s.OccType = occtype
	g := f.Geometry()
	if g.IsNull() || g.IsEmpty() {
		s.X = f.FieldAsFloat64(idxs[1])
		s.Y = f.FieldAsFloat64(idxs[2])
	} else {
		s.X = f.Geometry().X(0)
		s.Y = f.Geometry().Y(0)
	}
	s.DamCat = "RES"
	s.FoundType = f.FieldAsString(idxs[5])
	grndElev := f.FieldAsFloat64(idxs[7])
	ffe := f.FieldAsFloat64(idxs[9])
	s.StructVal = consequences.ParameterValue{Value: f.FieldAsFloat64(idxs[3])}
	s.ContVal = consequences.ParameterValue{Value: f.FieldAsFloat64(idxs[4])}
	s.FoundHt = consequences.ParameterValue{Value: ffe - grndElev}
	s.NumStories = int32(f.FieldAsInteger(idxs[6]))
	s.GroundElevation = grndElev
	s.ConstructionType = f.FieldAsString(idxs[8])

	return s, nil
}

func swapOcctypeMap(
	m map[string]structures.OccupancyTypeStochastic,
) map[string]structures.OccupancyTypeDeterministic {
	m2 := make(map[string]structures.OccupancyTypeDeterministic)
	for name, ot := range m {
		m2[name] = ot.CentralTendency()
	}
	return m2
}

func featuretoDeterministicStructure(
	f *gdal.Feature,
	m map[string]structures.OccupancyTypeDeterministic,
	defaultOcctype structures.OccupancyTypeDeterministic,
	idxs []int,
) (structures.StructureDeterministic, error) {
	defer f.Destroy()
	s := structures.StructureDeterministic{}
	s.Name = fmt.Sprintf("%v", f.FieldAsInteger(idxs[0]))
	OccTypeName := f.FieldAsString(idxs[5])
	var occtype = defaultOcctype
	//dont have access to foundation type in the structure schema yet.
	if idxs[9] > 0 {
		if otf, okf := m[OccTypeName+"-"+f.FieldAsString(idxs[9])]; okf {
			occtype = otf
		} else {
			if ot, ok := m[OccTypeName]; ok {
				occtype = ot
			} else {
				occtype = defaultOcctype
				msg := "Using default " + OccTypeName + " not found"
				fmt.Println(msg)
				//return s, errors.New(msg)
			}
		}
	} else {
		if ot, ok := m[OccTypeName]; ok {
			occtype = ot
		} else {
			occtype = defaultOcctype
			msg := "Using default " + OccTypeName + " not found"
			fmt.Println(msg)
			//return s, errors.New(msg)
		}
	}

	s.OccType = occtype
	g := f.Geometry()
	if g.IsNull() || g.IsEmpty() {
		s.X = f.FieldAsFloat64(idxs[1])
		s.Y = f.FieldAsFloat64(idxs[2])
	} else {
		s.X = f.Geometry().X(0)
		s.Y = f.Geometry().Y(0)
	}
	s.DamCat = "RES"
	s.FoundType = f.FieldAsString(idxs[5])
	grndElev := f.FieldAsFloat64(idxs[7])
	ffe := f.FieldAsFloat64(idxs[9])
	s.StructVal = f.FieldAsFloat64(idxs[3])
	s.ContVal = f.FieldAsFloat64(idxs[4])
	s.FoundHt = ffe - grndElev
	s.NumStories = int32(f.FieldAsInteger(idxs[6]))
	s.GroundElevation = grndElev
	s.ConstructionType = f.FieldAsString(idxs[8])
	return s, nil
}
