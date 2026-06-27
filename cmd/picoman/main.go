package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const version = "v0.1.119"

func main() {
	log.SetPrefix("picoman: ")

	// SSH_ASKPASS dispatch: when ssh-add execs us via the askpass symlink,
	// argv[0] ends in "-askpass" and argv[1] is the prompt text. Detect that
	// and act as the askpass helper before falling into the verb switch.
	if strings.HasSuffix(os.Args[0], "-askpass") {
		runAskpass()
		return
	}

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
	case "host":
		runHost(os.Args[2:])
	case "hosts":
		runHosts(os.Args[2:])
	case "group":
		runGroup(os.Args[2:])
	case "groups":
		runGroups(os.Args[2:])
	case "askpass":
		runAskpass()
	case "update":
		runUpdate(os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "Service:")
	fmt.Fprintln(os.Stderr, "  setup                              Interactive first-time setup")
	fmt.Fprintln(os.Stderr, "  install                            Install systemd user service")
	fmt.Fprintln(os.Stderr, "  uninstall                          Remove systemd user service")
	fmt.Fprintln(os.Stderr, "  start                              Run the daemon in the foreground")
	fmt.Fprintln(os.Stderr, "  stop                               Stop the systemd service")
	fmt.Fprintln(os.Stderr, "  restart                            Restart the systemd service")
	fmt.Fprintln(os.Stderr, "  status                             Show systemd service status")
	fmt.Fprintln(os.Stderr, "  version                            Print version")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Key state (daemon required):")
	fmt.Fprintln(os.Stderr, "  unseal [<passphrase>]              Unseal daemon (arg | piped stdin | configured/default command on tty)")
	fmt.Fprintln(os.Stderr, "  seal                               Forget key passphrase")
	fmt.Fprintln(os.Stderr, "  lock                               Unload SSH key")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Operations (require unlocked key):")
	fmt.Fprintln(os.Stderr, "  run <target> [<command>...]        Run remote command (or read command from stdin)")
	fmt.Fprintln(os.Stderr, "  get <target> <remote> [<local>]    Download from target into local work dir")
	fmt.Fprintln(os.Stderr, "  put <target> <local> [<remote>]    Upload to target work dir")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Host registry:")
	fmt.Fprintln(os.Stderr, "  hosts                              List configured targets")
	fmt.Fprintln(os.Stderr, "  host list                          List configured targets")
	fmt.Fprintln(os.Stderr, "  host info <name>                   Show one target")
	fmt.Fprintln(os.Stderr, "  host add                           Print bootstrap snippet (placeholder HOSTNAME)")
	fmt.Fprintln(os.Stderr, "  host add <name>                    Print bootstrap snippet for <name>")
	fmt.Fprintln(os.Stderr, "  host add <name> <user@host:port> <keytype> <key>")
	fmt.Fprintln(os.Stderr, "                                     Register target with pinned host key")
	fmt.Fprintln(os.Stderr, "  host note <name> [<note>]          Set or clear free-form note")
	fmt.Fprintln(os.Stderr, "  host set <name> remote_work_dir [<path>]")
	fmt.Fprintln(os.Stderr, "                                     Set or clear target remote work dir")
	fmt.Fprintln(os.Stderr, "  host remove <name>                 Remove target")
	fmt.Fprintln(os.Stderr, "  groups                             List configured groups")
	fmt.Fprintln(os.Stderr, "  group list                         List configured groups")
	fmt.Fprintln(os.Stderr, "  group info @<group>                Show group hosts")
	fmt.Fprintln(os.Stderr, "  group add @<group> <host>          Add host to group")
	fmt.Fprintln(os.Stderr, "  group remove @<group> <host>       Remove host from group")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Maintenance:")
	fmt.Fprintln(os.Stderr, "  loglevel <chat|all>                Set audit verbosity")
	fmt.Fprintln(os.Stderr, "  update [--help]                    Update from developer_dir or latest GitHub release")
	fmt.Fprintln(os.Stderr, "  fallback [<tag>]                   Install a specific GitHub release (no tag = list recent)")
}
