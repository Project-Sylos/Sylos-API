package terminal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

// Manager handles terminal-related operations
type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	logTerminal   *exec.Cmd
	logTerminalMu sync.Mutex
}

func NewManager(logger zerolog.Logger, cfg config.Config) *Manager {
	return &Manager{
		logger: logger,
		cfg:    cfg,
	}
}

// FindTerminalEmulator finds an available terminal emulator on the system.
// Returns the command name.
func FindTerminalEmulator() string {
	// Try common terminal emulators in order of preference
	terminals := []string{
		"gnome-terminal",
		"konsole",
		"xterm",
		"x-terminal-emulator", // Generic fallback on Debian/Ubuntu
	}

	for _, term := range terminals {
		if _, err := exec.LookPath(term); err == nil {
			return term
		}
	}

	return ""
}

// SpawnLogTerminal spawns the log terminal process if not already running.
// Returns an error if a terminal is already running on the given address.
func (m *Manager) SpawnLogTerminal(logAddress string) error {
	m.logTerminalMu.Lock()
	defer m.logTerminalMu.Unlock()

	if m.logTerminal != nil {
		// Check if process is still running
		if m.logTerminal.ProcessState == nil {
			// Process is still running (hasn't exited yet)
			return fmt.Errorf("log terminal already running on %s", logAddress)
		}
		// Process exited, clear it
		m.logTerminal = nil
	}

	// Find an available terminal emulator
	terminalCmd := FindTerminalEmulator()
	if terminalCmd == "" {
		return fmt.Errorf("no terminal emulator found (tried: xterm, gnome-terminal, konsole, x-terminal-emulator)")
	}

	// Get the current working directory to find the cmd directory
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}

	// Try to find cmd/spawn-log-terminal/main.go relative to current directory
	// First try current directory (if running from repo root)
	spawnPath := filepath.Join(cwd, "cmd", "spawn-log-terminal", "main.go")
	if _, err := os.Stat(spawnPath); os.IsNotExist(err) {
		// Fallback: try relative to executable
		execPath, err := os.Executable()
		if err == nil {
			execDir := filepath.Dir(execPath)
			spawnPath = filepath.Join(execDir, "..", "cmd", "spawn-log-terminal", "main.go")
			spawnPath = filepath.Clean(spawnPath)
		}
		// Check if it exists now
		if _, err := os.Stat(spawnPath); os.IsNotExist(err) {
			return fmt.Errorf("spawn-log-terminal not found at %s (ensure cmd/spawn-log-terminal/main.go exists)", spawnPath)
		}
	}

	// Build the command to run in the new terminal
	goRunCmd := fmt.Sprintf("go run %s %s", spawnPath, logAddress)

	// Prepare terminal command with arguments
	var cmd *exec.Cmd
	switch terminalCmd {
	case "gnome-terminal":
		cmd = exec.Command(terminalCmd, "--title=Log Terminal", "--", "sh", "-c", goRunCmd)
	case "konsole":
		cmd = exec.Command(terminalCmd, "-e", "sh", "-c", goRunCmd)
	case "xterm":
		cmd = exec.Command(terminalCmd, "-T", "Log Terminal", "-e", "sh", "-c", goRunCmd)
	default:
		// Generic x-terminal-emulator
		cmd = exec.Command(terminalCmd, "-e", "sh", "-c", goRunCmd)
	}

	// Don't attach stdout/stderr - let the terminal handle it
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start log terminal: %w", err)
	}

	m.logTerminal = cmd

	// Monitor process in background
	go func() {
		if err := cmd.Wait(); err != nil {
			m.logger.Warn().
				Err(err).
				Str("address", logAddress).
				Msg("log terminal process exited")
		}
		m.logTerminalMu.Lock()
		if m.logTerminal == cmd {
			m.logTerminal = nil
		}
		m.logTerminalMu.Unlock()
	}()

	m.logger.Info().
		Str("address", logAddress).
		Int("pid", cmd.Process.Pid).
		Msg("spawned log terminal")

	return nil
}

// StopLogTerminal stops the currently running log terminal process if any.
func (m *Manager) StopLogTerminal() error {
	m.logTerminalMu.Lock()
	defer m.logTerminalMu.Unlock()

	if m.logTerminal == nil {
		return nil
	}

	if m.logTerminal.ProcessState != nil && m.logTerminal.ProcessState.Exited() {
		m.logTerminal = nil
		return nil
	}

	if err := m.logTerminal.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill log terminal: %w", err)
	}

	m.logger.Info().
		Int("pid", m.logTerminal.Process.Pid).
		Msg("stopped log terminal")

	m.logTerminal = nil
	return nil
}

// ToggleLogTerminal toggles the log terminal on or off.
func (m *Manager) ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error {
	if enable {
		if logAddress == "" {
			logAddress = m.cfg.Runtime.LogAddress
		}
		if logAddress == "" {
			return fmt.Errorf("log address is required")
		}
		return m.SpawnLogTerminal(logAddress)
	}
	return m.StopLogTerminal()
}
