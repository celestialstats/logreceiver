package main

import (
	log "github.com/Sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)
	log.Info("--- Celestial Stats Log Receiver ---")
	flag.Parse()
	if *clientId == "" {
		*clientId = os.Getenv("DISCORD_CLIENTID")
	}
	if *clientSecret == "" {
		*clientSecret = os.Getenv("DISCORD_CLIENTSECRET")
	}
	if *botToken == "" {
		*botToken = os.Getenv("DISCORD_BOTTOKEN")
	}
	if *logDir == "" {
		*logDir = os.Getenv("LOGDIR")
	}
	log.Info("Launch Parameters:")
	log.Info("\tDISCORD_CLIENTID:", *clientId)
	log.Info("\tDISCORD_CLIENTSECRET:", *clientSecret)
	log.Info("\tDISCORD_BOTTOKEN:", *botToken)
	log.Info("\tLOGDIR:", *logDir)

	log.Info("Log Receiver is now running.  Press CTRL-C to exit.")

	<-quit

	log.Info("Exiting...")
	return
}
