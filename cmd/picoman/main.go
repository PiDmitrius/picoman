package main

import (
	"fmt"
	"log"
	"os"
)

const version = "v0.1.62"

func main() {
	log.SetPrefix("picoman: ")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		runDaemon()
	case "stop":
		runServiceCtl("stop")
	case "restart":
		runServiceCtl("restart")
	case "status":
		runStatus()
	case "install":
		runInstall()
	case "uninstall":
		runUninstall()
	case "setup":
		runSetup()
	case "unseal":
		runUnseal(os.Args[2:])
	case "seal":
		runSeal()
	case "unlock":
		runUnlock(os.Args[2:])
	case "lock":
		runLock()
	case "run":
		runLocalRun(os.Args[2:])
	case "get":
		runLocalGet(os.Args[2:])
	case "put":
		runLocalPut(os.Args[2:])
	case "loglevel":
		runLogLevel(os.Args[2:])
	case "askpass":
		runAskpass()
	case "update":
		runUpdate()
	case "fallback":
		runFallback(os.Args[2:])
	case "version":
		fmt.Printf("picoman %s\n", version)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "picoman %s — Telegram-controlled SSH key opener\n\n", version)
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  setup       Interactive first-time setup")
	fmt.Fprintln(os.Stderr, "  install     Install systemd user service")
	fmt.Fprintln(os.Stderr, "  uninstall   Remove systemd user service")
	fmt.Fprintln(os.Stderr, "  start       Run the service")
	fmt.Fprintln(os.Stderr, "  unseal      Send key passphrase to the running daemon")
	fmt.Fprintln(os.Stderr, "  seal        Forget key passphrase in the running daemon")
	fmt.Fprintln(os.Stderr, "  unlock      Load SSH key into daemon-controlled ssh-agent")
	fmt.Fprintln(os.Stderr, "  lock        Remove SSH key from daemon-controlled ssh-agent")
	fmt.Fprintln(os.Stderr, "  run         Run command on a configured target")
	fmt.Fprintln(os.Stderr, "  get         Copy remote work file into local work directory")
	fmt.Fprintln(os.Stderr, "  put         Copy local work file into target work directory")
	fmt.Fprintln(os.Stderr, "  loglevel    Set audit log level: chat or all")
	fmt.Fprintln(os.Stderr, "  stop        Stop the service")
	fmt.Fprintln(os.Stderr, "  restart     Restart the service")
	fmt.Fprintln(os.Stderr, "  update      Update from source_dir or latest GitHub release")
	fmt.Fprintln(os.Stderr, "  fallback    Install a specific GitHub release")
	fmt.Fprintln(os.Stderr, "  status      Show service status")
	fmt.Fprintln(os.Stderr, "  version     Print version")
}
