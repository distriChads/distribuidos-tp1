package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("log", "level")
	v.BindEnv("worker", "exchange", "input", "name")
	v.BindEnv("worker", "exchange", "input", "routingkeys")
	v.BindEnv("worker", "exchange", "secondinput", "name")
	v.BindEnv("worker", "exchange", "secondinput", "routingkeys")
	v.BindEnv("worker", "exchange", "output", "name")
	v.BindEnv("worker", "exchange", "output", "routingkeys")
	v.BindEnv("worker", "queue", "name")
	v.BindEnv("worker", "broker")
	v.BindEnv("worker", "maxmessages")
	v.BindEnv("worker", "expectedeof")
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
	return nil
}
