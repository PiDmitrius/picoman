package agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// CleanLegacy removes credential capability artifacts created by releases
// that used an external ssh-agent.
func CleanLegacy(socket string) CleanResult {
	if strings.TrimSpace(socket) == "" {
		return CleanResult{Agent: "none", Socket: "absent", PIDFile: "absent", Askpass: "absent"}
	}
	pidPath := socket + ".pid"
	result := CleanResult{Agent: stopLegacyAgent(pidPath)}
	result.Socket = removeLegacyPath(socket)
	result.PIDFile = removeLegacyPath(pidPath)
	result.Askpass = removeLegacyPath(socket + "-askpass")
	return result
}

func stopLegacyAgent(pidPath string) string {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return "none"
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return "invalid pid file"
	}
	cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return "none"
	}
	if !isLegacySSHAgentCmdline(cmdline) {
		return fmt.Sprintf("pid %d is not ssh-agent, left untouched", pid)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Sprintf("find pid %d: %v", pid, err)
	}
	if err := process.Kill(); err != nil {
		return fmt.Sprintf("kill pid %d: %v", pid, err)
	}
	return fmt.Sprintf("killed pid %d", pid)
}

func isLegacySSHAgentCmdline(cmdline []byte) bool {
	fields := strings.Split(string(cmdline), "\x00")
	if len(fields) == 0 {
		return false
	}
	name := fields[0]
	if slash := strings.LastIndexByte(name, '/'); slash >= 0 {
		name = name[slash+1:]
	}
	return name == "ssh-agent"
}

func removeLegacyPath(path string) string {
	err := os.Remove(path)
	switch {
	case err == nil:
		return "removed"
	case os.IsNotExist(err):
		return "absent"
	default:
		return "remove failed: " + err.Error()
	}
}
