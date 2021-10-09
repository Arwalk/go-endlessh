package main

import (
	"fmt"
	"github.com/go-ini/ini"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

type Configuration struct {
	statsFile  *os.File
	logFile    *os.File
	port    string
	bindFamily string
	maxClients uint32
	delay uint32
	maxLineLength uint32
}

func checkConfigFile(config *ini.File) (Configuration, error) {
	var conf = Configuration{
		statsFile:  nil,
		logFile:    nil,
		port:    "",
		maxClients: 0,
		bindFamily: "",
		delay : 0,
		maxLineLength: 0,
	}

	var checkUintOk = func (section string, key string, onErr string, onOk func(value uint32)) error {
		value , err := config.Section(section).Key(key).Uint()
		if err != nil {
			return fmt.Errorf("%s : %s", onErr, err)
		}
		onOk(uint32(value))
		return err
	}

	var confItems = map[string]func(string, string) error{
		"server/port": func(section string, key string) error {
			return checkUintOk(section, key, "invalid port value, could not parse to int", func(value uint32) {
				conf.port = config.Section(section).Key(key).String()
			})
		},
		"server/maxClients": func(section string, key string) error {
			return checkUintOk(section, key, "invalid maxClients value %s in config, expected an integer", func(value uint32) {
				conf.maxClients = value
			})
		},
		"server/bindFamily": func(section string, key string) error {
			var bindFamily = config.Section(section).Key(key).String()
			if !func(a string, list []string) bool {
				for _, b := range list {
					if b == a {
						return true
					}
				}
				return false
			}(bindFamily, []string{"all", "tcp4", "tcp6"}) {
				return fmt.Errorf("invalid bindFamily value %s in config, expected all, tcp4 or tcp6", bindFamily)
			}
			if bindFamily == "all" {
				bindFamily = "tcp"
			}
			conf.bindFamily = bindFamily
			return nil
		},
		"paths/statsFile": func(section string, key string) error {
			var err error = nil
			var statsFile = config.Section(section).Key(key).String()
			conf.statsFile, err = os.OpenFile(statsFile, os.O_RDWR|os.O_CREATE, 0755)
			if err != nil {
				return fmt.Errorf("invalid statsFile value %s in config, see error %s", statsFile, err)
			}
			return nil
		},
		"paths/logFile": func(section string, key string) error {
			var err error = nil
			var logFile = config.Section(section).Key(key).String()
			conf.logFile, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("invalid logFile value %s in config, see error %s", logFile, err)
			}
			return nil
		},
		"tarPit/delay" : func(section string, key string) error {
			return checkUintOk(section, key, "invalid delay value in config, see error", func(value uint32) {
				conf.delay = value
			})
		},
		"tarPit/MaxLineLength" : func(section string, key string) error {
			return checkUintOk(section, key, "invalid MaxLineLength value in config", func(value uint32) {
				conf.maxLineLength = value
			})
		},
	}

	var getItemPath = func(confPath string) (string, string) {
		var split = strings.Split(confPath, "/")
		return split[0], split[1]
	}

	for confElem, _ := range confItems {
		section, key := getItemPath(confElem)
		if !config.Section(section).HasKey(key) {
			return conf, fmt.Errorf("missing configuration item %s", confElem)
		}
	}

	for confElem, checker := range confItems {
		section, key := getItemPath(confElem)
		err := checker(section, key)
		if err != nil {
			return conf, err
		}
	}

	return conf, nil
}

var activeClients int32 = 0

func pushClient() {
	atomic.AddInt32(&activeClients, 1)
}

func popClient() {
	atomic.AddInt32(&activeClients, -1)
}

func getClientCount() int32 {
	return atomic.LoadInt32(&activeClients)
}

func main() {

	log.Info("Starting go-endlessh service")

	path, err := os.Getwd()
	checkError(err)

	configFile, err := ini.Load(fmt.Sprintf("%s/config.ini", path))
	checkError(err)

	conf, err := checkConfigFile(configFile)
	checkError(err)

	tcpAddr, err := net.ResolveTCPAddr(conf.bindFamily, fmt.Sprintf(":%s", conf.port))
	checkError(err)

	listener, err := net.ListenTCP(conf.bindFamily, tcpAddr)
	checkError(err)

	log.Info("Tarpit is running")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		for getClientCount() == int32(conf.maxClients) {
			time.Sleep(20 * time.Millisecond)
		}

		pushClient()

		go handleClient(conn, conf)
	}
}

const lineBreak byte = '\n'

func generateMessage(maxLineLength uint32) []byte {
	var numBytes = 2 + rand.Intn(int(maxLineLength))
	var message = make([]byte, numBytes+1)
	for i := 0; i <= len(message) - 2; i++ {
		var value byte = 0
		for {
			value = byte(rand.Intn(256))
			if value != lineBreak {
				break
			}
		}
		message[i] = value
	}
	message[len(message) - 1] = lineBreak

	return message
}

func handleClient(conn net.Conn, configuration Configuration) {
	// close connection on exit
	defer func(conn net.Conn) {
		_ = conn.Close()
	}(conn)
	defer popClient()

	log.WithFields(log.Fields{
		"event": "Accept",
		"host":  conn.RemoteAddr(),
	}).Info("")


	var totalBytes uint64 = 0

	for {
		written, err := conn.Write(generateMessage(configuration.maxLineLength)) // don't care about return value
		if err != nil {
			break
		}
		totalBytes = totalBytes + uint64(written)
		time.Sleep(time.Duration(configuration.delay) * time.Millisecond)
	}

	log.WithFields(log.Fields{
		"event": "Close",
		"host":  conn.RemoteAddr(),
	}).Info()
}

func checkError(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
