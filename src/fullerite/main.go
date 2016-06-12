package main

import (
	"fullerite/config"
	"fullerite/handler"
	"fullerite/internalserver"
	"fullerite/metric"

	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/davecheney/profile"
)

const (
	name    = "fullerite"
	version = "0.6.3"
	desc    = "Diamond compatible metrics collector"
)

var log = logrus.WithFields(logrus.Fields{"app": "fullerite"})

func initLogrus(ctx *cli.Context) {
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		TimestampFormat: time.RFC822,
		FullTimestamp:   true,
	})

	if level, err := logrus.ParseLevel(ctx.String("log_level")); err == nil {
		logrus.SetLevel(level)
	} else {
		log.Error(err)
		logrus.SetLevel(logrus.InfoLevel)
	}

	logrus.SetOutput(os.Stdout)
}

func main() {
	app := cli.NewApp()
	app.Name = name
	app.Version = version
	app.Usage = desc
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/fullerite.conf",
			Usage: "JSON formatted configuration file",
		},
		cli.StringFlag{
			Name:  "log_level, l",
			Value: "info",
			Usage: "Logging level (debug, info, warn, error, fatal, panic)",
		},
		cli.BoolFlag{
			Name:  "profile",
			Usage: "Enable profiling",
		},
	}
	app.Action = start

	commandFlags := []cli.Flag{
		cli.IntFlag{
			Name:  "die-after, d",
			Value: 600,
			Usage: "How long (in seconds) to run the collector",
		},
		cli.IntFlag{
			Name:  "interval, i",
			Value: 10,
			Usage: "How frequent (in seconds) to run your collector",
		},
	}
	commandFlags = append(commandFlags, app.Flags...)
	app.Commands = []cli.Command{
		{
			Name:    "visualize",
			Action:  visualize,
			Aliases: []string{"visualise", "vis", "viz"},
			Flags:   commandFlags,
			Usage:   "shortest path from your terminal to your graphs",
			UsageText: "You can use this tool to run a script that returns JSON\n" +
				"as per the schema defined at \n" +
				"https://github.com/Yelp/fullerite/tree/master/src/fullerite/examples/adhoc/schema.json\n" +
				"This JSON will be read from stdout and passed through to\n" +
				"the fullerite TCP port on localhost to send to your graphing backend.\n" +
				"All metric names produced will be prepended with your username as per\n" +
				"the output of `whoami`. This is to make your metrics easier to find\n" +
				"and also to avoid polluting other metrics that exist with the same name\n\n\n" +
				"NOTE: Make sure you flush out all your metrics either as a list OR individually separated\n" +
				"with a newline '\\n'otherwise your metrics will not be parsed and will be IGNORED\n",
		},
	}
	app.Run(os.Args)
}

func start(ctx *cli.Context) {
	if ctx.Bool("profile") {
		pcfg := profile.Config{
			CPUProfile:   true,
			MemProfile:   true,
			BlockProfile: true,
			ProfilePath:  ".",
		}
		p := profile.Start(&pcfg)
		defer p.Stop()
	}
	quit := make(chan bool)
	initLogrus(ctx)
	log.Info("Starting fullerite...")

	c, err := config.ReadConfig(ctx.String("config"))
	if err != nil {
		return
	}
	collectors := startCollectors(c)
	handlers := startHandlers(c)
	collectorStatChan := make(chan metric.CollectorEmission)

	internalServer := internalserver.New(c,
		handlerStatFunc(handlers),
		readCollectorStat(collectorStatChan))
	go internalServer.Run()

	readFromCollectors(collectors, handlers, collectorStatChan)

	hook := NewLogErrorHook(handlers)
	log.Logger.Hooks.Add(hook)

	<-quit
}

func handlerStatFunc(handlers []handler.Handler) internalserver.InternalStatFunc {
	return func() map[string]metric.InternalMetrics {
		stats := map[string]metric.InternalMetrics{}
		for _, inst := range handlers {
			stats[inst.Name()] = inst.InternalMetrics()
		}
		return stats
	}
}

func readCollectorStat(collectorStatChan <-chan metric.CollectorEmission) internalserver.InternalStatFunc {
	collectorMetrics := map[string]uint64{}
	go func() {
		for collectorMetric := range collectorStatChan {
			collectorMetrics[collectorMetric.Name] = collectorMetric.EmissionCount
		}
	}()
	return func() map[string]metric.InternalMetrics {
		metricStats := map[string]metric.InternalMetrics{}
		for k, v := range collectorMetrics {
			counters := map[string]float64{"fullerite.collector_datapoints": float64(v)}
			gauges := map[string]float64{}

			m := metric.InternalMetrics{
				Counters: counters,
				Gauges:   gauges,
			}
			metricStats[k] = m
		}
		return metricStats
	}
}

func visualize(ctx *cli.Context) {
	initLogrus(ctx)
	log.Info("Visualizing fullerite...")

	if len(ctx.Args()) == 0 {
		log.Error("You need a collector file to visualize!, see 'fullerite help visualize'")
		return
	}

	c, err := config.ReadConfig(ctx.String("config"))
	if err != nil {
		return
	}

	// Setup AdHoc Collector config from context and args
	collectorFile, _ := filepath.Abs(ctx.Args()[0])
	configMap := make(map[string]interface{})
	configMap["interval"] = ctx.Int("interval")
	configMap["collectorFile"] = collectorFile

	// Start collector and handlers
	collector := startCollector("AdHoc", c, configMap)
	c.Collectors = []string{"AdHoc"}
	c.DiamondCollectors = []string{}
	handlers := startHandlers(c)

	// Read the metrics from the AdHoc collector
	go readFromCollector(collector, handlers)

	// Stop collecting after `die-after` duration expires
	quitChannel := make(chan bool, 1)
	defer close(quitChannel)

	dieAfter := time.Duration(ctx.Int("die-after"))
	time.AfterFunc(dieAfter*time.Second, func() {
		log.Info("Quitting...")
		quitChannel <- true
	})
	// Wait to quit
	<-quitChannel
}
