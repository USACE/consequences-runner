package actions

import (
	"fmt"
	"path/filepath"

	"github.com/usace-cloud-compute/cc-go-sdk"
)

const (
	postActionName string = "post-outputs"
)

func init() {
	cc.ActionRegistry.RegisterAction(postActionName, &PostAction{})
}

type PostAction struct {
	cc.ActionRunnerBase
}

func (pa *PostAction) Run() error {
	extension := ""
	pm := pa.PluginManager
	pl := pm.Payload
	for _, o := range pl.Outputs {
		for i, dp := range o.Paths {
			extension = filepath.Ext(dp)
			cfri := cc.CopyFileToRemoteInput{
				RemoteStoreName: o.StoreName,
				RemotePath:      dp,
				LocalPath:       fmt.Sprintf("%v/%v%v", localData, o.Name, extension),
				RemoteDsName:    o.Name,
				DsPathKey:       i,
				DsDataPathKey:   "",
			}
			err := pm.CopyFileToRemote(cfri)
			if err != nil {
				return err
			}
		}

	}
	return nil
}
