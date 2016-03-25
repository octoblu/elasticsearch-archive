package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/coreos/go-semver/semver"
	"github.com/fatih/color"
	"github.com/octoblu/elasticsearch-archive/elasticsearch"
	De "github.com/tj/go-debug"
)

var debug = De.Debug("elasticsearch-archive:main")

func main() {
	yesterday := time.Now().AddDate(0, 0, -1)

	app := cli.NewApp()
	app.Name = "elasticsearch-archive"
	app.Version = version()
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "expression, e",
			EnvVar: "ELASTICSEARCH_ARCHIVE_EXPRESSION",
			Usage:  "Expression to snapshot. '2016-03-12' will create a snapshot called 'auto-snapshot-2016-03-12' with every index containing 2016-03-12 in its name. Defaults to yesterday",
			Value:  fmt.Sprintf("%d-%02d-%02d", yesterday.Year(), yesterday.Month(), yesterday.Day()),
		},
		cli.StringFlag{
			Name:   "repository, r",
			EnvVar: "ELASTICSEARCH_ARCHIVE_REPOSITORY",
			Usage:  "Example string flag",
		},
		cli.StringFlag{
			Name:   "uri, u",
			EnvVar: "ELASTICSEARCH_ARCHIVE_URI",
			Usage:  "Elasticsearch uri",
		},
	}
	app.Run(os.Args)
}

func run(context *cli.Context) {
	expression, repository, uri := getOpts(context)

	client := elasticsearch.New(uri, repository)
	if err := client.Snapshot(expression); err != nil {
		log.Fatalln("Error during snapshot:", err.Error())
	}
}

func getOpts(context *cli.Context) (string, string, string) {
	expression := context.String("expression")
	repository := context.String("repository")
	uri := context.String("uri")

	if repository == "" || uri == "" {
		cli.ShowAppHelp(context)

		if repository == "" {
			color.Red("  Missing required flag --repository or ELASTICSEARCH_ARCHIVE_REPOSITORY")
		}
		if uri == "" {
			color.Red("  Missing required flag --uri or ELASTICSEARCH_ARCHIVE_URI")
		}
		os.Exit(1)
	}

	if expression == "" {
	}

	return expression, repository, uri
}

func version() string {
	version, err := semver.NewVersion(VERSION)
	if err != nil {
		errorMessage := fmt.Sprintf("Error with version number: %v", VERSION)
		log.Panicln(errorMessage, err.Error())
	}
	return version.String()
}
