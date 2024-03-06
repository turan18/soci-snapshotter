package lod

import (
	"github.com/urfave/cli"
)

const (
	Version       = "0.1"
	SpanOrderType = "Span"
)

type LOD struct {
	Version   string     `json:"version"`
	OrderType OrderType  `json:"orderType"`
	SpanList  []SpanItem `json:"spanList,omitempty"`
}

type OrderType string

type SpanItem struct {
	Id          uint64
	LayerDigest string
}

var Command = cli.Command{
	Name:  "lod",
	Usage: "manage lod",
	Subcommands: []cli.Command{
		createCommand,
	},
}
