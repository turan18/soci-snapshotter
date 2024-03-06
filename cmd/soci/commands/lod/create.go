package lod

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/awslabs/soci-snapshotter/cmd/soci/commands/internal"
	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/urfave/cli"
)

const (
	SyncFetchLog = "synchronously fetching span"
)

type SociLog struct {
	Msg string `json:"msg"`
}

type Info struct {
	Layer string `json:"layer"`
	Id    uint64 `json:"spanId"`
}

var createCommand = cli.Command{
	Name:        "create",
	Usage:       "create a LOD",
	Description: "create a LOD (Load Order Document) with offline snapshotter log parsing",
	ArgsUsage:   "<soci_debug_log_path>",
	Action: func(cliContext *cli.Context) error {
		_, cancel := internal.AppContext(cliContext)
		defer cancel()
		logPath := cliContext.Args().First()
		_, err := os.Stat(logPath)
		if err != nil {
			return err
		}
		f, err := os.Open(logPath)
		if err != nil {
			return err
		}
		l := &soci.LOD{
			Version:   soci.Version,
			OrderType: soci.SpanOrderType,
		}
		// Parse log
		s := &SociLog{}
		i := &Info{}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			log := scanner.Bytes()
			err := json.Unmarshal(log, s)
			if err != nil {
				return err
			}
			if s.Msg == SyncFetchLog {
				err := json.Unmarshal(log, i)
				if err != nil {
					return err
				}
				l.SpanList = append(l.SpanList, soci.SpanItem{
					Id:          i.Id,
					LayerDigest: i.Layer,
				})
			}
		}
		// Create blob
		lodBlob, err := json.MarshalIndent(l, "", "   ")
		if err != nil {
			return err
		}
		if err = os.WriteFile("lod.json", lodBlob, 0644); err != nil {
			return err
		}
		// Serialize it as an image manifest

		return nil
	},
}
