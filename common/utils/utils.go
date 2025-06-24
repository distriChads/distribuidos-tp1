package utils

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var logger *logging.Logger

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("log", "level")
	v.BindEnv("heartbeat", "port")
	v.BindEnv("worker", "exchange", "input", "routingkeys")
	v.BindEnv("worker", "exchange", "output", "routingkeys")
	v.BindEnv("worker", "broker")
	v.BindEnv("worker", "maxmessages")
	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead\n")
	}

	// Print all settings loaded by Viper (including env vars)
	// This is useful for debugging
	fmt.Println("--- Viper Settings ---")
	for key, value := range v.AllSettings() {
		fmt.Printf("%s: %v\n", key, value)
	}
	fmt.Println("----------------------")

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	logger = logging.MustGetLogger("utils")
	return nil
}

func HeartBeat(ctx context.Context, port int) {
	if logger == nil {
		fmt.Println("Logger not initialized")
		return
	}

	// Create UDP connection
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		logger.Fatal("Error listening on UDP port:", err)
	}
	defer conn.Close()

	logger.Infof("HeartBeat server listening on port %d", port)

	buffer := make([]byte, 5)

	for {
		// Set read timeout to 1 second
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		select {
		case <-ctx.Done():
			logger.Info("HeartBeat server shutting down")
			return
		default:
		}
		// Read message from UDP connection
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Ignore timeout errors
				continue
			}
			logger.Warningf("Error reading from UDP: %v", err)
			continue
		}
		if n < 5 {
			logger.Warningf("Short read from %v: %s", remoteAddr, string(buffer[:n]))
			continue
		}
		if buffer[4] != 0 {
			logger.Warningf("Invalid message from %v: %s", remoteAddr, string(buffer[:n]))
			continue
		}
		message := string(buffer[:n])
		logger.Debugf("Received from %v: %s", remoteAddr, message)

		// Send "OK" response back to the sender
		response := []byte{'P', 'O', 'N', 'G', 0}
		for i := range 3 {

			sent := 0
			for sent < len(response) {
				i, err := conn.WriteToUDP(response[sent:], remoteAddr)
				if err != nil {
					break
				}
				sent += i
			}

			if sent != len(response) {
				logger.Warningf("Error sending response (attempt %d): %v", i+1, err)
				if i == 2 {
					logger.Warningf("Failed to send response after 3 attempts")
				} else {
					time.Sleep(time.Duration(2+float64(i)*1.2) * time.Second)
					sent = 0
				}
			} else {
				logger.Debugf("Sent response to %v: %s", remoteAddr, string(response[:sent]))
				break
			}
		}
	}
}
