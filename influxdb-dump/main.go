package main

import (
  "flag"
  "fmt"
  "io"
  "log"
  "os"
  "time"
  "github.com/eckardt/influxdb-go"
)

type ClientConfig struct {
  *influxdb.ClientConfig
  Destination string
  StartTime string
  EndTime string
}

type Client struct {
  *influxdb.Client
  *ClientConfig
}

func parseFlags() (*ClientConfig) {
  config := &ClientConfig{&influxdb.ClientConfig{}, "", "", ""}
  flag.StringVar(&config.Host, "host", "localhost:8086", "host to connect to")
  flag.StringVar(&config.Username, "username", "root", "username to authenticate as")
  flag.StringVar(&config.Password, "password", "root", "password to authenticate with")
  flag.StringVar(&config.Database, "database", "", "database to dump")
  flag.StringVar(&config.Destination, "out", "-", "output file (default to stdout)")
  flag.StringVar(&config.StartTime, "start", "", "start time for dump (format 2006-01-02T15:05:05)")
  flag.StringVar(&config.EndTime, "end", "", "end time for dump (format 2006-01-02T15:05:05)")
  flag.BoolVar(&config.IsSecure, "https", false, "connect via https")
  flag.Parse()
  if config.Database == "" {
    fmt.Fprintln(os.Stderr, "flag is mandatory but not provided: -database")
    flag.Usage()
    os.Exit(1)
  }
  return config
}

func main() {
  config := parseFlags()
  _client, err := influxdb.NewClient(config.ClientConfig)
  if err != nil {
    log.Fatal(err)
  }
  client := Client{_client, config}
  client.DumpSeries()
}

func (self *Client) DumpSeries() {
  var err error
  var file io.Writer
  var start time.Time
  var end time.Time
  query := "SELECT * from /.*/"
  if self.StartTime != "" {
    start, err = time.Parse("2006-01-02T15:04:05", self.StartTime)
    if err != nil {
      log.Fatal(err)
      return
    }
    query += " where time > '" + start.Format("2006-01-02 15:04:05") + "'"
  }
  if self.EndTime != "" {
    end, err = time.Parse("2006-01-02T15:04:05", self.EndTime)
    if err != nil {
      log.Fatal(err)
      return
    }
    if start.IsZero() {
      query += " where time < '" + end.Format("2006-01-02 15:04:05") + "'"
    } else {
      query += " and time < '" + end.Format("2006-01-02 15:04:05") + "'"
    }
  }
  if start.IsZero() == false && end.IsZero() == false {
    diff := end.Sub(start)
    if diff < 0 {
      log.Fatal("End time must be greater than start time")
      return
    }
  }
  if self.Destination != "-" {
    file, err = os.Create(self.Destination)
    if err != nil {
      log.Fatal(err)
    }
  } else {
    file = os.Stdout
  }
  err = self.QueryStream(query, file)
  if err != nil {
    log.Fatal(err)
  }
}
