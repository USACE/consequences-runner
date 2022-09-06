package consequences

type Model struct{
	StructureInventoryPath string `json:"structure_inventory_path"`
}

func (m Model) Compute(seeds plugin.SeedSet, hp hazards.HazardProvider) error{
	//need to get hazard provider and structure provider connected, and produce output to an output stream
}